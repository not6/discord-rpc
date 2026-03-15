#pragma once
#include <stdint.h>
#include <stdbool.h>

// clang-format off

#if defined(DISCORD_DYNAMIC_LIB)
#  if defined(_WIN32)
#    if defined(DISCORD_BUILDING_SDK)
#      define DISCORD_EXPORT __declspec(dllexport)
#    else
#      define DISCORD_EXPORT __declspec(dllimport)
#    endif
#  else
#    define DISCORD_EXPORT __attribute__((visibility("default")))
#  endif
#else
#  define DISCORD_EXPORT
#endif

// clang-format on

#ifdef __cplusplus
extern "C" {
#endif

enum class DiscordJoinResponse : int8_t {
    No,
    Yes,
    Ignore, // same as no
};

enum class DiscordPartyPrivacy : int8_t {
    Private,
    Public,
};

enum class DiscordActivityActionType : int8_t {
    Join = 1,
    Spectate,
};

enum class DiscordActivityType : int8_t {
    Playing,
    Streaming,
    Listening,
    Watching,
    Custom,
    Competing,
};

enum class DiscordActivityFlags : uint32_t {
    None                     = 0,
    Instance                 = 1UL << 0,
    Join                     = 1UL << 1,
    Spectate                 = 1UL << 2,
    JoinRequest              = 1UL << 3,
    Sync                     = 1UL << 4,
    Play                     = 1UL << 5,
    PartyPrivacyFriends      = 1UL << 6,
    PartyPrivacyVoiceChannel = 1UL << 7,
    Embedded                 = 1UL << 8,
};

typedef struct DiscordButton {
    const char* label;
    const char* url;
} DiscordButton;

typedef struct DiscordRichPresence {
    DiscordActivityType type;
    const char* state;      /* max 128 bytes */
    const char* stateUrl;   /* max 128 bytes */
    const char* details;    /* max 128 bytes */
    const char* detailsUrl; /* max 128 bytes */
    int64_t startTimestamp;
    int64_t endTimestamp;
    const char* largeImageKey;  /* max 32 bytes */
    const char* largeImageText; /* max 128 bytes */
    const char* smallImageKey;  /* max 32 bytes */
    const char* smallImageText; /* max 128 bytes */
    const char* partyId;        /* max 128 bytes */
    int partySize;
    int partyMax;
    DiscordPartyPrivacy partyPrivacy;
    const char* matchSecret;    /* max 128 bytes */
    const char* joinSecret;     /* max 128 bytes */
    const char* spectateSecret; /* max 128 bytes */
    bool instance;
    DiscordActivityFlags flags;
    const DiscordButton* buttons;
} DiscordRichPresence;

typedef struct DiscordUser {
    const char* userId;
    const char* username;
    const char* discriminator;
    const char* globalName;
    const char* avatar;
} DiscordUser;

typedef struct DiscordEventHandlers {
    void (*ready)(const DiscordUser* user);
    void (*disconnected)(int errorCode, const char* message);
    void (*errored)(int errorCode, const char* message);
    void (*debug)(bool isOut, const char* opcodeName, const char* message, uint32_t messageLength);
    void (*joinGame)(const char* joinSecret);
    void (*spectateGame)(const char* spectateSecret);
    void (*joinRequest)(const DiscordUser* user);
    void (*invited)(DiscordActivityActionType type,
                    const DiscordUser* user,
                    const DiscordRichPresence* activity,
                    const char* sessionId,
                    const char* channelId,
                    const char* messageId);
} DiscordEventHandlers;

enum class DiscordConnectionUpdateType : int8_t {
    Full,
    ReadOnly,
    WriteOnly,
};

DISCORD_EXPORT void Discord_Initialize(const char* applicationId,
                                       DiscordEventHandlers* handlers,
                                       bool autoRegister,
                                       const char* optionalSteamId);
DISCORD_EXPORT void Discord_Shutdown(void);

/* checks for incoming messages, dispatches callbacks */
DISCORD_EXPORT void Discord_RunCallbacks(void);

/* If you disable the lib starting its own io thread, you'll need to call this from your own */
#ifdef DISCORD_DISABLE_IO_THREAD
DISCORD_EXPORT void Discord_UpdateConnection(DiscordConnectionUpdateType type = DiscordConnectionUpdateType::Full);
DISCORD_EXPORT bool Discord_ConnectionHasPendingSends(void);
#endif

DISCORD_EXPORT void Discord_UpdatePresence(const DiscordRichPresence* presence);
DISCORD_EXPORT void Discord_ClearPresence(void);

DISCORD_EXPORT void Discord_Respond(const char* userid, DiscordJoinResponse reply);

DISCORD_EXPORT void Discord_AcceptInvite(const char* userId,
                                         DiscordActivityActionType type,
                                         const char* sessionId,
                                         const char* channelId,
                                         const char* messageId);

DISCORD_EXPORT void Discord_OpenActivityInvite(DiscordActivityActionType type);
DISCORD_EXPORT void Discord_OpenGuildInvite(const char* code);

DISCORD_EXPORT void Discord_UpdateHandlers(DiscordEventHandlers* handlers);

#ifdef __cplusplus
} /* extern "C" */
#endif
