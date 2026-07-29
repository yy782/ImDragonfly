#include <string_view>

struct RESP2Parser {
  const char* p = nullptr;
  const char* end = nullptr;

  explicit RESP2Parser(std::string_view data)
      : p(data.data()), end(data.data() + data.size()) {}
  std::string_view NextElement() {
    if (p >= end) return {};
    char type = *p++;
    switch (type) {
      case '$': {
        int len = ReadInt();
        if (len < 0) {
          return {};
        }
        const char* data = p;
        p += len;
        if (p + 2 <= end) p += 2;
        return std::string_view(data, len);
      }
      case '*': {
        int count = ReadInt();

        (void)count;
        return {};
      }
      default:
        return {};
    }
  }

  int ReadArrayLen() {
    if (p >= end || *p != '*') return -1;
    ++p;
    return ReadInt();
  }

 private:
  int ReadInt() {
    int v = 0;
    bool neg = false;
    if (p < end && *p == '-') {
      neg = true;
      ++p;
    }
    while (p < end && *p >= '0' && *p <= '9') {
      v = v * 10 + (*p++ - '0');
    }
    if (p + 1 < end) p += 2;  // skip \r\n
    return neg ? -v : v;
  }
};