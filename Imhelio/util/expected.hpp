#pragma once
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
namespace utils{

template <typename E>
class unexpected {
public:
    explicit unexpected(const E& e) : err_(e) {}
    explicit unexpected(E&& e) : err_(std::move(e)) {}
    
    const E& value() const { return err_; }
    E& value() { return err_; }
    
private:
    E err_;
};


template <typename E>
unexpected<typename std::decay<E>::type> make_unexpected(E&& e) {
    return unexpected<typename std::decay<E>::type>(std::forward<E>(e));
}


template <typename T, typename E>
class expected {
private:
    
    union Storage {
        T value;
        E error;
        
        Storage() {}
        ~Storage() {}
    };
    
    Storage storage_;
    bool has_value_ : 1;  
    bool engaged_ : 1;    

    void destroy() {
        if (engaged_) {
            if (has_value_) {
                storage_.value.~T();
            } else {
                storage_.error.~E();
            }
            engaged_ = false;
        }
    }
    
public:
    expected() : has_value_(true), engaged_(true) {
        new (&storage_.value) T();
    }
    
    template<typename U>
    expected(U&& u) : has_value_(true), engaged_(true) {
        new (&storage_.value) T(u);
    }
    
    expected(const unexpected<E>& u) : has_value_(false), engaged_(true) {
        new (&storage_.error) E(u.value());
    }
    
    expected(unexpected<E>&& u) : has_value_(false), engaged_(true) {
        new (&storage_.error) E(std::move(u.value()));
    }
    
    expected(const expected& other) : has_value_(other.has_value_), engaged_(true) {
        if (has_value_) {
            new (&storage_.value) T(other.storage_.value);
        } else {
            new (&storage_.error) E(other.storage_.error);
        }
    }
    
    expected(expected&& other) noexcept(
        std::is_nothrow_move_constructible<T>::value &&
        std::is_nothrow_move_constructible<E>::value
    ) : has_value_(other.has_value_), engaged_(true) {
        if (has_value_) {
            new (&storage_.value) T(std::move(other.storage_.value));
        } else {
            new (&storage_.error) E(std::move(other.storage_.error));
        }
        other.engaged_ = false;
    }
    
    ~expected() {
        destroy();
    }
    
    
    expected& operator=(const expected& other) {
        if (this != &other) {
            destroy();
            has_value_ = other.has_value_;
            engaged_ = true;
            if (has_value_) {
                new (&storage_.value) T(other.storage_.value);
            } else {
                new (&storage_.error) E(other.storage_.error);
            }
        }
        return *this;
    }
    
    expected& operator=(expected&& other) noexcept(
        std::is_nothrow_move_constructible<T>::value &&
        std::is_nothrow_move_constructible<E>::value
    ) {
        if (this != &other) {
            destroy();
            has_value_ = other.has_value_;
            engaged_ = true;
            if (has_value_) {
                new (&storage_.value) T(std::move(other.storage_.value));
            } else {
                new (&storage_.error) E(std::move(other.storage_.error));
            }
            other.engaged_ = false;
        }
        return *this;
    }
    
    
    
    bool has_value() const { return has_value_; }
    explicit operator bool() const { return has_value_; }

    const T& value() const {
        if (!has_value_) {
            throw std::runtime_error("expected has error, not value");
        }
        return storage_.value;
    }
    
    T& value() {
        if (!has_value_) {
            throw std::runtime_error("expected has error, not value");
        }
        return storage_.value;
    }
    
    
    const E& error() const {
        if (has_value_) {
            throw std::runtime_error("expected has value, not error");
        }
        return storage_.error;
    }
    
    E& error() {
        if (has_value_) {
            throw std::runtime_error("expected has value, not error");
        }
        return storage_.error;
    }
    
    
    const T& operator*() const { return storage_.value; }
    T& operator*() { return storage_.value; }
    
    const T* operator->() const { return &storage_.value; }
    T* operator->() { return &storage_.value; }
    
    
    operator T() const {
        return value();
    }
    
    expected& operator=(const T& v) {
        destroy();
        has_value_ = true;
        engaged_ = true;
        new (&storage_.value) T(v);
        return *this;
    }
    
    expected& operator=(T&& v) {
        destroy();
        has_value_ = true;
        engaged_ = true;
        new (&storage_.value) T(std::move(v));
        return *this;
    }
    
    expected& operator=(const unexpected<E>& u) {
        destroy();
        has_value_ = false;
        engaged_ = true;
        new (&storage_.error) E(u.value());
        return *this;
    }
    
    expected& operator=(unexpected<E>&& u) {
        destroy();
        has_value_ = false;
        engaged_ = true;
        new (&storage_.error) E(std::move(u.value()));
        return *this;
    }
};


template <typename T, typename E>
expected<T, E> make_expected(T&& v) {
    return expected<T, E>(std::forward<T>(v));
}


template <typename E, typename T>
expected<T, E> make_expected_from_error(T&& v) {
    return expected<T, E>(unexpected<E>(std::forward<T>(v)));
}

template <typename T, typename E>
bool operator==(const expected<T, E>& a, const expected<T, E>& b) {
    if (a.has_value() != b.has_value()) return false;
    if (a.has_value()) {
        return *a == *b;
    } else {
        return a.error() == b.error();
    }
}

template <typename T, typename E>
bool operator!=(const expected<T, E>& a, const expected<T, E>& b) {
    return !(a == b);
}





}