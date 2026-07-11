#include "discord_rpc.h"

#include "backoff.h"
#include "discord_register.h"
#include "msg_queue.h"
#include "rpc_connection.h"
#include "serialization.h"

#include <stdio.h>

#include <memory>
#include <atomic>
#include <chrono>
#include <mutex>

#ifndef DISCORD_DISABLE_IO_THREAD
#include <condition_variable>
#include <thread>
#endif

#include "deleters.h"

constexpr size_t MaxMessageSize{16 * 1024};
constexpr size_t MessageQueueSize{8};
constexpr size_t JoinQueueSize{8};
constexpr size_t InviteQueueSize{8};

struct QueuedMessage {
    size_t length;
    char buffer[MaxMessageSize];

    void Copy(const QueuedMessage& other)
    {
        length = other.length;
        if (length) {
            memcpy(buffer, other.buffer, length);
        }
    }
};

struct User {
    // snowflake (64bit int), turned into a ascii decimal string, at most 20 chars +1 null
    // terminator = 21
    char userId[32];
    // 32 unicode glyphs is max name size => 4 bytes per glyph in the worst case, +1 for null
    // terminator = 129
    char username[344];
    // 4 decimal digits + 1 null terminator = 5
    char discriminator[8];
    // 32 unicode glyphs is max name size => 4 bytes per glyph in the worst case, +1 for null
    // terminator = 129 (TODO: is thhat correct?)
    char globalName[344];
    // optional 'a_' + md5 hex digest (32 bytes) + null terminator = 35
    char avatar[128];
    // Rounded way up because I'm paranoid about games breaking from future changes in these sizes
};

struct Activity {
    DiscordActivityType type;
    DiscordActivityFlags flags;
    DiscordStatusDisplayType statusDisplayType;
    char name[128];
    char details[128];
    char detailsUrl[128];
    char state[128];
    char stateUrl[128];
    int64_t startTimestamp;
    int64_t endTimestamp;
    //char largeImageKey[32];
    //char largeImageText[128];
    //char largeImageUrl[128];
    //char smallImageKey[32];
    //char smallImageText[128];
    //char smallImageUrl[128];
    //char inviteCoverImageKey[32];
    char partyId[128];
    int partySize;
    int partyMax;
    //DiscordPartyPrivacy partyPrivacy;
    //char matchSecret[128];
    //char joinSecret[128];
    //char spectateSecret[128];
    //char emojiName[128];
    //char emojiId[64];
    //bool emojiAnimated;
    //bool instance;
    //DiscordButton buttons[2];
};

struct Invite {
    User user;
    Activity activity;
    DiscordActivityActionType type;
    char sessionId[128];
    char channelId[128];
    char messageId[128];
};

static RpcConnection* Connection{nullptr};
static DiscordEventHandlers QueuedHandlers{};
static DiscordEventHandlers Handlers{};
static std::atomic_bool WasJustConnected{false};
static std::atomic_bool WasJustDisconnected{false};
static std::atomic_bool GotErrorMessage{false};
static std::atomic_bool WasJoinGame{false};
static std::atomic_bool WasSpectateGame{false};
static std::atomic_bool UpdatePresence{false};
static char JoinGameSecret[256];
static char SpectateGameSecret[256];
static int LastErrorCode{0};
static char LastErrorMessage[256];
static int LastDisconnectErrorCode{0};
static char LastDisconnectErrorMessage[256];
static std::mutex PresenceMutex;
static std::mutex HandlerMutex;
static QueuedMessage QueuedPresence{};
static MsgQueue<QueuedMessage, MessageQueueSize> SendQueue;
static MsgQueue<User, JoinQueueSize> JoinAskQueue;
static MsgQueue<Invite, InviteQueueSize> InviteQueue;
static User connectedUser;

// We want to auto connect, and retry on failure, but not as fast as possible. This does expoential
// backoff from 0.5 seconds to 1 minute
static Backoff ReconnectTimeMs(500, 60 * 1000);
static auto NextConnect = std::chrono::system_clock::now();
static int Pid{0};
static int Nonce{1};

#ifndef DISCORD_DISABLE_IO_THREAD
static void Discord_UpdateConnection(DiscordConnectionUpdateType type = DiscordConnectionUpdateType::Full);
static void Discord_RunCallbacks(void);

class IoThreadHolder {
private:
    std::atomic_bool keepRunning;
    std::mutex waitForIOMutex;
    std::condition_variable waitForIOActivity;
    std::thread ioThread;

public:
    void Start()
    {
        keepRunning.store(true);
        ioThread = std::thread([&]() {
            const std::chrono::duration<int64_t, std::milli> maxWait{500LL};
            Discord_UpdateConnection();
            Discord_RunCallbacks();
            while (keepRunning.load()) {
                std::unique_lock<std::mutex> lock(waitForIOMutex);
                waitForIOActivity.wait_for(lock, maxWait);
                Discord_UpdateConnection();
                Discord_RunCallbacks();
            }
        });
    }

    void Notify() { waitForIOActivity.notify_all(); }

    void Stop()
    {
        keepRunning.exchange(false);
        Notify();
        if (ioThread.joinable()) {
            ioThread.join();
        }
    }

    ~IoThreadHolder() { Stop(); }
};
#else
class IoThreadHolder {
public:
    void Notify() {}
    void Start() {}
    void Stop() {}
};
#endif // DISCORD_DISABLE_IO_THREAD
static IoThreadHolder IoThread{};

static void UpdateReconnectTime()
{
    NextConnect = std::chrono::system_clock::now() +
      std::chrono::duration<int64_t, std::milli>{ReconnectTimeMs.nextDelay()};
}

#ifdef DISCORD_DISABLE_IO_THREAD
extern "C" DISCORD_EXPORT bool Discord_ConnectionHasPendingSends(void)
{
    return (UpdatePresence.load() && QueuedPresence.length) || SendQueue.HavePendingSends();
}
#endif

#ifdef DISCORD_DISABLE_IO_THREAD
extern "C" DISCORD_EXPORT void Discord_UpdateConnection(DiscordConnectionUpdateType type/* = DiscordConnectionUpdateType::Full*/)
#else
static void Discord_UpdateConnection(DiscordConnectionUpdateType type/* = DiscordConnectionUpdateType::Full*/)
#endif
{
    if (!Connection) {
        return;
    }

    if (!Connection->IsOpen()) {
        if (std::chrono::system_clock::now() >= NextConnect) {
            UpdateReconnectTime();
            Connection->Open();
        }
    }
    else {
        // reads
        if (type != DiscordConnectionUpdateType::WriteOnly) {
            for (;;) {
                static alignas(JsonDocument) uint8_t buffer[sizeof(JsonDocument)];
                std::unique_ptr<JsonDocument, destruct_only_deleter<JsonDocument>> message{new (buffer) JsonDocument};

                if (!Connection->Read(*message)) {
                    break;
                }

                const char* evtName = GetStrMember(message.get(), "evt");
                const char* nonce = GetStrMember(message.get(), "nonce");

                if (nonce) {
                    // in responses only -- should use to match up response when needed.

                    if (evtName && strcmp(evtName, "ERROR") == 0) {
                        auto data = GetObjMember(message.get(), "data");
                        LastErrorCode = GetIntMember(data, "code");
                        StringCopy(LastErrorMessage, GetStrMember(data, "message", ""));
                        GotErrorMessage.store(true);
                    }
                }
                else {
                    // should have evt == name of event, optional data
                    if (evtName == nullptr) {
                        continue;
                    }

                    auto data = GetObjMember(message.get(), "data");

                    if (strncmp(evtName, "ACTIVITY_", 9) == 0) {
                        const char* activityName = &evtName[9];
                        if (strcmp(activityName, "JOIN") == 0) {
                            auto secret = GetStrMember(data, "secret");
                            if (secret) {
                                StringCopy(JoinGameSecret, secret);
                                WasJoinGame.store(true);
                            }
                        }
                        else if (strcmp(activityName, "SPECTATE") == 0) {
                            auto secret = GetStrMember(data, "secret");
                            if (secret) {
                                StringCopy(SpectateGameSecret, secret);
                                WasSpectateGame.store(true);
                            }
                        }
                        else if (strcmp(activityName, "JOIN_REQUEST") == 0) {
                            auto user = GetObjMember(data, "user");
                            auto userId = GetStrMember(user, "id");
                            auto username = GetStrMember(user, "username");
                            auto joinReq = JoinAskQueue.GetNextAddMessage();
                            if (userId && username && joinReq) {
                                StringCopy(joinReq->userId, userId);
                                StringCopy(joinReq->username, username);
                                StringCopyOptional(joinReq->discriminator,
                                                GetStrMember(user, "discriminator"));
                                StringCopyOptional(joinReq->globalName, GetStrMember(user, "global_name"));
                                StringCopyOptional(joinReq->avatar, GetStrMember(user, "avatar"));
                                JoinAskQueue.CommitAdd();
                            }
                        }
                        else if (strcmp(activityName, "INVITE") == 0) {
                            auto inviteReq = InviteQueue.GetNextAddMessage();
                            if (inviteReq) {
                                memset(inviteReq, 0, sizeof(*inviteReq));
                                auto user = GetObjMember(data, "user");
                                auto userId = GetStrMember(user, "id");
                                auto username = GetStrMember(user, "username");
                                if (userId && username) {
                                    StringCopy(inviteReq->user.userId, userId);
                                    StringCopy(inviteReq->user.username, username);
                                    StringCopyOptional(inviteReq->user.discriminator,
                                                    GetStrMember(user, "discriminator"));
                                    StringCopyOptional(inviteReq->user.globalName,
                                                    GetStrMember(user, "global_name"));
                                    StringCopyOptional(inviteReq->user.avatar,
                                                    GetStrMember(user, "avatar"));
                                }
                                auto activity = GetObjMember(data, "activity");
                                if (activity) {
                                    inviteReq->activity.type = (DiscordActivityType)GetIntMember(activity, "type", (int)DiscordActivityType::Playing);
                                    inviteReq->activity.flags = (DiscordActivityFlags)GetIntMember(activity, "flags", (int)DiscordActivityFlags::None);
                                    inviteReq->activity.statusDisplayType = (DiscordStatusDisplayType)GetIntMember(activity, "status_display_type", (int)DiscordStatusDisplayType::Name);
                                    StringCopyOptional(inviteReq->activity.name,
                                                    GetStrMember(activity, "name"));
                                    StringCopyOptional(inviteReq->activity.details,
                                                    GetStrMember(activity, "details"));
                                    StringCopyOptional(inviteReq->activity.detailsUrl,
                                                    GetStrMember(activity, "details_url"));
                                    StringCopyOptional(inviteReq->activity.state,
                                                    GetStrMember(activity, "state"));
                                    StringCopyOptional(inviteReq->activity.stateUrl,
                                                    GetStrMember(activity, "state_url"));
                                    auto timestamps = GetObjMember(activity, "timestamps");
                                    if (timestamps) {
                                        inviteReq->activity.startTimestamp =
                                        GetInt64Member(timestamps, "start");
                                        inviteReq->activity.endTimestamp =
                                        GetInt64Member(timestamps, "end");
                                    }
                                    /*
                                    auto assets = GetObjMember(activity, "assets");
                                    if (assets) {
                                        StringCopyOptional(inviteReq->activity.largeImageKey,
                                                        GetStrMember(assets, "large_image"));
                                        StringCopyOptional(inviteReq->activity.largeImageText,
                                                        GetStrMember(assets, "large_text"));
                                        StringCopyOptional(inviteReq->activity.largeImageUrl,
                                                        GetStrMember(assets, "large_url"));
                                        StringCopyOptional(inviteReq->activity.smallImageKey,
                                                        GetStrMember(assets, "small_image"));
                                        StringCopyOptional(inviteReq->activity.smallImageText,
                                                        GetStrMember(assets, "small_text"));
                                        StringCopyOptional(inviteReq->activity.smallImageUrl,
                                                        GetStrMember(assets, "small_url"));
                                        StringCopyOptional(inviteReq->activity.inviteCoverImageKey,
                                                        GetStrMember(assets, "invite_cover_image"));
                                    }
                                    */
                                    auto party = GetObjMember(activity, "party");
                                    if (party) {
                                        StringCopyOptional(inviteReq->activity.partyId,
                                                        GetStrMember(party, "id"));
                                        auto size_ = GetAnyMember(party, "size");
                                        if (size_->IsArray()) {
                                            auto size = size_->GetArray();
                                            if (size.Size() >= 2) {
                                                if (size[0].IsInt()) {
                                                    inviteReq->activity.partySize = size[0].GetInt();
                                                }
                                                if (size[1].IsInt()) {
                                                    inviteReq->activity.partyMax = size[1].GetInt();
                                                }
                                            }
                                        }
                                    }
                                    /*
                                    auto emoji = GetObjMember(data, "emoji");
                                    if (emoji) {
                                        StringCopyOptional(inviteReq->activity.emojiName,
                                                        GetStrMember(emoji, "name"));
                                        StringCopyOptional(inviteReq->activity.emojiId,
                                                        GetStrMember(emoji, "id"));
                                        inviteReq->activity.emojiAnimated = GetBoolMember(emoji, "animated");
                                    }
                                    */
                                    /*
                                    // need to change the Activity struct for this
                                    auto buttons = GetObjMember(data, "buttons");
                                    if (buttons) {
                                        StringCopyOptional(inviteReq->activity.buttons[0].label,
                                                        GetStrMember(buttons, "label"));
                                        StringCopyOptional(inviteReq->activity.buttons[0].url,
                                                        GetStrMember(buttons, "url"));
                                    }
                                    */
                                }
                                inviteReq->type = (DiscordActivityActionType)GetIntMember(data, "type");
                                StringCopyOptional(inviteReq->channelId, GetStrMember(user, "channel_id"));
                                StringCopyOptional(inviteReq->messageId, GetStrMember(user, "message_id"));
                                InviteQueue.CommitAdd();
                            }
                        }
                    }
                }
            }
        }

        // writes
        if (type != DiscordConnectionUpdateType::ReadOnly) {
            if (UpdatePresence.exchange(false) && QueuedPresence.length) {
                static QueuedMessage local;
                {
                    std::lock_guard<std::mutex> guard(PresenceMutex);
                    local.Copy(QueuedPresence);
                }
                if (!Connection->Write(local.buffer, local.length)) {
                    // if we fail to send, requeue
                    std::lock_guard<std::mutex> guard(PresenceMutex);
                    QueuedPresence.Copy(local);
                    UpdatePresence.exchange(true);
                }
            }

            while (SendQueue.HavePendingSends()) {
                auto qmessage = SendQueue.GetNextSendMessage();
                Connection->Write(qmessage->buffer, qmessage->length);
                SendQueue.CommitSend();
            }
        }
    }
}

static void SignalIOActivity()
{
    IoThread.Notify();
}

static bool RegisterForEvent(const char* evtName)
{
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length =
          JsonWriteSubscribeCommand(qmessage->buffer, sizeof(qmessage->buffer), Nonce++, evtName);
        SendQueue.CommitAdd();
        SignalIOActivity();
        return true;
    }
    return false;
}

static bool DeregisterForEvent(const char* evtName)
{
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length =
          JsonWriteUnsubscribeCommand(qmessage->buffer, sizeof(qmessage->buffer), Nonce++, evtName);
        SendQueue.CommitAdd();
        SignalIOActivity();
        return true;
    }
    return false;
}

extern "C" DISCORD_EXPORT void Discord_Initialize(const char* applicationId,
                                                  DiscordEventHandlers* handlers,
                                                  bool autoRegister,
                                                  const char* optionalSteamId)
{
    if (autoRegister) {
        if (optionalSteamId && optionalSteamId[0]) {
            Discord_RegisterSteamGame(applicationId, optionalSteamId);
        }
        else {
            Discord_Register(applicationId, nullptr);
        }
    }

    Pid = GetProcessId();

    {
        std::lock_guard<std::mutex> guard(HandlerMutex);

        if (handlers) {
            QueuedHandlers = *handlers;
        }
        else {
            QueuedHandlers = {};
        }

        Handlers = {};
    }

    if (Connection) {
        return;
    }

    Connection = RpcConnection::Create(applicationId);
    Connection->onConnect = [](JsonDocument& readyMessage) {
        Discord_UpdateHandlers(&QueuedHandlers);
        if (QueuedPresence.length > 0) {
            UpdatePresence.exchange(true);
            SignalIOActivity();
        }
        auto data = GetObjMember(&readyMessage, "data");
        auto user = GetObjMember(data, "user");
        auto userId = GetStrMember(user, "id");
        auto username = GetStrMember(user, "username");
        if (userId && username) {
            StringCopy(connectedUser.userId, userId);
            StringCopy(connectedUser.username, username);
            StringCopyOptional(connectedUser.discriminator, GetStrMember(user, "discriminator"));
            StringCopyOptional(connectedUser.globalName, GetStrMember(user, "global_name"));
            StringCopyOptional(connectedUser.avatar, GetStrMember(user, "avatar"));
        }
        WasJustConnected.exchange(true);
        ReconnectTimeMs.reset();
    };
    Connection->onDisconnect = [](int err, const char* message) {
        LastDisconnectErrorCode = err;
        StringCopy(LastDisconnectErrorMessage, message);
        WasJustDisconnected.exchange(true);
        UpdateReconnectTime();
    };
    Connection->onDebug = [](bool out, RpcConnection::MessageFrame* frame) {
        if (Handlers.debug) {
            const char* opcode = "Unknown";
            switch (frame->opcode) {
            case RpcConnection::Opcode::Handshake:
                opcode = "Handshake";
                break;
            case RpcConnection::Opcode::Frame:
                opcode = "Frame";
                break;
            case RpcConnection::Opcode::Close:
                opcode = "Close";
                break;
            case RpcConnection::Opcode::Ping:
                opcode = "Ping";
                break;
            case RpcConnection::Opcode::Pong:
                opcode = "Pong";
                break;
            }
            Handlers.debug(out, opcode, frame->message, frame->length);
        }
    };

    IoThread.Start();
}

extern "C" DISCORD_EXPORT void Discord_Shutdown(void)
{
    if (!Connection) {
        return;
    }
    Connection->onConnect = nullptr;
    Connection->onDisconnect = nullptr;
    Connection->onDebug = nullptr;
    Handlers = {};
    QueuedPresence.length = 0;
    UpdatePresence.exchange(false);
    IoThread.Stop();
    SendQueue.Reset();

    RpcConnection::Destroy(Connection);
}

extern "C" DISCORD_EXPORT void Discord_UpdatePresence(const DiscordRichPresence* presence)
{
    {
        std::lock_guard<std::mutex> guard(PresenceMutex);
        QueuedPresence.length = JsonWriteRichPresenceObj(
          QueuedPresence.buffer, sizeof(QueuedPresence.buffer), Nonce++, Pid, presence);
        UpdatePresence.exchange(true);
    }
    SignalIOActivity();
}

extern "C" DISCORD_EXPORT void Discord_ClearPresence(void)
{
    Discord_UpdatePresence(nullptr);
}

extern "C" DISCORD_EXPORT void Discord_Respond(const char* userId, DiscordJoinResponse reply)
{
    // if we are not connected, let's not batch up stale messages for later
    if (!Connection || !Connection->IsOpen()) {
        return;
    }
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length =
          JsonWriteJoinReply(qmessage->buffer, sizeof(qmessage->buffer), userId, reply, Nonce++);
        SendQueue.CommitAdd();
        SignalIOActivity();
    }
}

extern "C" DISCORD_EXPORT void Discord_AcceptInvite(const char* userId,
                                                    DiscordActivityActionType type,
                                                    const char* sessionId,
                                                    const char* channelId,
                                                    const char* messageId)
{
    // if we are not connected, let's not batch up stale messages for later
    if (!Connection || !Connection->IsOpen()) {
        return;
    }
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length = JsonWriteAcceptInvite(qmessage->buffer,
                                                 sizeof(qmessage->buffer),
                                                 userId,
                                                 type,
                                                 sessionId,
                                                 channelId,
                                                 messageId,
                                                 Nonce++);
        SendQueue.CommitAdd();
        SignalIOActivity();
    }
}

extern "C" DISCORD_EXPORT void Discord_OpenActivityInvite(DiscordActivityActionType type)
{
    // if we are not connected, let's not batch up stale messages for later
    if (!Connection || !Connection->IsOpen()) {
        return;
    }
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length = JsonWriteOpenOverlayActivityInvite(
          qmessage->buffer, sizeof(qmessage->buffer), type, Nonce++, Pid);
        SendQueue.CommitAdd();
        SignalIOActivity();
    }
}

extern "C" DISCORD_EXPORT void Discord_OpenGuildInvite(const char* code)
{
    // if we are not connected, let's not batch up stale messages for later
    if (!Connection || !Connection->IsOpen()) {
        return;
    }
    auto qmessage = SendQueue.GetNextAddMessage();
    if (qmessage) {
        qmessage->length = JsonWriteOpenOverlayGuildInvite(
          qmessage->buffer, sizeof(qmessage->buffer), code, Nonce++, Pid);
        SendQueue.CommitAdd();
        SignalIOActivity();
    }
}

#ifdef DISCORD_DISABLE_IO_THREAD
extern "C" DISCORD_EXPORT void Discord_RunCallbacks(void)
#else
static void Discord_RunCallbacks(void)
#endif
{
    // Note on some weirdness: internally we might connect, get other signals, disconnect any number
    // of times inbetween calls here. Externally, we want the sequence to seem sane, so any other
    // signals are book-ended by calls to ready and disconnect.

    if (!Connection) {
        return;
    }

    bool wasDisconnected = WasJustDisconnected.exchange(false);
    bool isConnected = Connection->IsOpen();

    if (isConnected && wasDisconnected) { // wasDisconnected moved here to avoid unneeded lock_guard
        // if we are connected, disconnect cb first
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (/*wasDisconnected &&*/ Handlers.disconnected) {
            Handlers.disconnected(LastDisconnectErrorCode, LastDisconnectErrorMessage);
        }
    }

    if (WasJustConnected.exchange(false)) {
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (Handlers.ready) {
            DiscordUser du{connectedUser.userId,
                           connectedUser.username,
                           connectedUser.discriminator,
                           connectedUser.globalName,
                           connectedUser.avatar};
            Handlers.ready(&du);
        }
    }

    if (GotErrorMessage.exchange(false)) {
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (Handlers.errored) {
            Handlers.errored(LastErrorCode, LastErrorMessage);
        }
    }

    if (WasJoinGame.exchange(false)) {
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (Handlers.joinGame) {
            Handlers.joinGame(JoinGameSecret);
        }
    }

    if (WasSpectateGame.exchange(false)) {
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (Handlers.spectateGame) {
            Handlers.spectateGame(SpectateGameSecret);
        }
    }

    // Right now this batches up any requests and sends them all in a burst; I could imagine a world
    // where the implementer would rather sequentially accept/reject each one before the next invite
    // is sent. I left it this way because I could also imagine wanting to process these all and
    // maybe show them in one common dialog and/or start fetching the avatars in parallel, and if
    // not it should be trivial for the implementer to make a queue themselves.
    while (JoinAskQueue.HavePendingSends()) {
        auto req = JoinAskQueue.GetNextSendMessage();
        {
            std::lock_guard<std::mutex> guard(HandlerMutex);
            if (Handlers.joinRequest) {
                DiscordUser du{
                  req->userId, req->username, req->discriminator, req->globalName, req->avatar};
                Handlers.joinRequest(&du);
            }
        }
        JoinAskQueue.CommitSend();
    }

    while (InviteQueue.HavePendingSends()) {
        auto req = InviteQueue.GetNextSendMessage();
        {
            std::lock_guard<std::mutex> guard(HandlerMutex);
            if (Handlers.invited) {
                auto& u = req->user;
                DiscordUser du{u.userId, u.username, u.discriminator, u.globalName, u.avatar};
                auto& a = req->activity;
                DiscordRichPresence drp{a.type,
                                        a.flags,
                                        a.statusDisplayType,
                                        a.name,
                                        a.details,
                                        a.detailsUrl,
                                        a.state,
                                        a.stateUrl,
                                        a.startTimestamp,
                                        a.endTimestamp,
                                        nullptr, // a.largeImageKey,
                                        nullptr, // a.largeImageText,
                                        nullptr, // a.largeImageUrl,
                                        nullptr, // a.smallImageKey,
                                        nullptr, // a.smallImageText,
                                        nullptr, // a.smallImageUrl,
                                        nullptr, // a.inviteCoverImageKey,
                                        a.partyId,
                                        a.partySize,
                                        a.partyMax,
                                        DiscordPartyPrivacy::Private,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        //nullptr,
                                        //nullptr,
                                        //false,
                                        false,
                                        nullptr};
                Handlers.invited(
                  req->type, &du, &drp, req->sessionId, req->channelId, req->messageId);
            }
        }
        InviteQueue.CommitSend();
    }

    if (!isConnected) {
        // if we are not connected, disconnect message last
        std::lock_guard<std::mutex> guard(HandlerMutex);
        if (wasDisconnected && Handlers.disconnected) {
            Handlers.disconnected(LastDisconnectErrorCode, LastDisconnectErrorMessage);
        }
    }
}

extern "C" DISCORD_EXPORT void Discord_UpdateHandlers(DiscordEventHandlers* newHandlers)
{
    if (newHandlers) {
#define HANDLE_EVENT_REGISTRATION(handler_name, event)              \
    if (!Handlers.handler_name && newHandlers->handler_name) {      \
        RegisterForEvent(event);                                    \
    }                                                               \
    else if (Handlers.handler_name && !newHandlers->handler_name) { \
        DeregisterForEvent(event);                                  \
    }

        std::lock_guard<std::mutex> guard(HandlerMutex);
        HANDLE_EVENT_REGISTRATION(joinGame, "ACTIVITY_JOIN")
        HANDLE_EVENT_REGISTRATION(spectateGame, "ACTIVITY_SPECTATE")
        HANDLE_EVENT_REGISTRATION(joinRequest, "ACTIVITY_JOIN_REQUEST")
        HANDLE_EVENT_REGISTRATION(invited, "ACTIVITY_INVITE")

#undef HANDLE_EVENT_REGISTRATION

        Handlers = *newHandlers;
    }
    else {
        std::lock_guard<std::mutex> guard(HandlerMutex);
        Handlers = {};
    }
    return;
}
