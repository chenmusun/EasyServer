#ifndef BASE64_LIB_H_
#define BASE64_LIB_H_
#include <string>

std::string base64_encode(unsigned char const* , unsigned int len);
std::string base64_decode(std::string const& s);
#endif
