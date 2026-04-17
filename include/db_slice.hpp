#include "DbTable.hpp"

class EngineShard;

class DbSlice 
{
    DbSlice(const DbSlice&) = delete;
    void operator=(const DbSlice&) = delete;

private:
    EngineShard* owner_;
    
};