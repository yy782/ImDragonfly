#pragma once
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
namespace util{

// TODO expected 不能区别有没有结果


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
    bool has_value_ = false;



    void destroy() {
        if (has_value_) {
            storage_.value.~T();
        } else {
            storage_.error.~E();
        }
    }
    
public:
    expected() : has_value_(true){
        new (&storage_.value) T();
    }
    
    template<typename U>
    expected(U&& u) : has_value_(true) {
        new (&storage_.value) T(u);
    }
    
    expected(const unexpected<E>& u) : has_value_(false){
        new (&storage_.error) E(u.value());
    }
    
    expected(unexpected<E>&& u) : has_value_(false){
        new (&storage_.error) E(std::move(u.value()));
    }
    
    expected(const expected& other) : has_value_(other.has_value_) {
        if (has_value_) {
            new (&storage_.value) T(other.storage_.value);
        } else {
            new (&storage_.error) E(other.storage_.error);
        }
    }
    
    expected(expected&& other) noexcept(
        std::is_nothrow_move_constructible<T>::value &&
        std::is_nothrow_move_constructible<E>::value
    ) : has_value_(other.has_value_) {
        if (has_value_) {
            new (&storage_.value) T(std::move(other.storage_.value));
        } else {
            new (&storage_.error) E(std::move(other.storage_.error));
        }
    }
    
    ~expected() {
        destroy();
    }
    
    
    expected& operator=(const expected& other) {
        if (this != &other) {
            destroy();
            has_value_ = other.has_value_;
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
            if (has_value_) {
                new (&storage_.value) T(std::move(other.storage_.value));
            } else {
                new (&storage_.error) E(std::move(other.storage_.error));
            }
        }
        return *this;
    }
    
    
    
    bool has_value() const { return has_value_; }
    explicit operator bool() const { return has_value_; }

    const T& value() const {
        if (!has_value_) {
            throw std::runtime_error("expected has error_, not value");
        }
        return storage_.value;
    }
    
    T& value() { // 移动会更好吗
        if (!has_value_) {
            throw std::runtime_error("expected has error_, not value");
        }
        return storage_.value;
    }
    
    
    const E& error_() const {
        if (has_value_) {
            throw std::runtime_error("expected has value, not error_");
        }
        return storage_.error;
    }
    
    E& error() {
        if (has_value_) {
            throw std::runtime_error("expected has value, not error_");
        }
        return storage_.error;
    }
    
};


template <typename E>
class expected<void, E> {
private:
    E error_; 
    void destroy() {
        error_.~E();
    }
    
public:
    expected() = default;
    
    expected(unexpected<E>&& u) {
        new (&error_) E(std::move(u.value()));
    }
    
    expected(const expected& other){
        // TODO 检查other是否有错误，error是不是空
        new (&error_) E(other.error_);
    }
    
    expected(expected&& other) = delete;
    expected& operator=(const expected& other) = delete;
    expected& operator=(expected&& other) = delete;    
    ~expected() {
        destroy();
    }
    
    

    bool has_value() const { return false; }
    explicit operator bool() const { return error_ == E{}; } // 注意这里的实现，表示没有错误时为true

    const E& error() const {
        return error_;
    }
    
    E& error() {
        return error_;
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
        return a.error_() == b.error_();
    }
}

template <typename T, typename E>
bool operator!=(const expected<T, E>& a, const expected<T, E>& b) {
    return !(a == b);
}





}