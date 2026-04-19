
enum class OpStatus : uint16_t {
    OK,
    KEY_NOTFOUND,
    WRONG_TYPE,
    OUT_OF_MEMORY,
};

// 简化版 OpResult
template<typename T>
class OpResult {
public:

    
    // 错误构造
    OpResult(Status status) : status_(status), has_value_(false) {}
    
    // 值构造
    OpResult(T&& value) : value_(std::move(value)), status_(Status::OK), has_value_(true) {}
    OpResult(const T& value) : value_(value), status_(Status::OK), has_value_(true) {}
    
    bool ok() const { return status_ == Status::OK && has_value_; }
    Status status() const { return status_; }
    
    T& value() { 
        if (!ok()) throw std::runtime_error("No value");
        return value_; 
    }
    
    T value_or(T default_val) const {
        return ok() ? value_ : default_val;
    }
    
    T* operator->() { return &value(); }
    T& operator*() { return value(); }
    
private:
    T value_{};
    OpStatus status_ = Status::OK;
    bool has_value_ = false;
};