typedef i16 short
typedef i32 int
typedef i64 long
typedef bool boolean
typedef string String

struct Person {
    1: optional String username,
    2: optional int age,
    3: optional boolean married
}
/*
* 此文件用于定义序列化存入kafka  topic的数据结构
*/