#include <type_traits>

template <class T, std::enable_if_t<std::is_destructible_v<T>, int> = 0>
struct destruct_only_deleter {
    constexpr destruct_only_deleter() noexcept = default;

    template <class T2, std::enable_if_t<
        std::conjunction_v<std::is_convertible<T2*, T*>, std::is_destructible<T2>>, int> = 0>
    _CONSTEXPR23 destruct_only_deleter(const destruct_only_deleter<T2>&) noexcept {}

    _CONSTEXPR23 void operator()(T* ptr) const noexcept { ptr->~T(); } // destructors can't throw
};
