#ifndef STRUCTS_TO_JSON_HPP_
#define STRUCTS_TO_JSON_HPP_
#include<string>
#include <jsoncpp/json/json.h>
#include"visit_struct.hpp"

struct SelfDefinedClass{

};

struct ConvertObjectToJson{
        ConvertObjectToJson(Json::Value * root){
                root_=root;
        }

        template<typename T>
        void convert_impl(const char * name,const T& value,std::true_type)
                {
                        Json::Value item;
                        visit_struct::apply_visitor(ConvertObjectToJson(&item),value);
                        (*root_)[name]=item;
                }

        template<typename T>
        void convert_impl(const char * name,const T& value,std::false_type)
                {
                        (*root_)[name]=value;
                }

        template<typename T>
        void convert_impl(const char *name,const std::vector<T>& value,std::false_type)
                {
                        Json::Value arr;
                        for(auto pos=value.begin();pos!=value.end();++pos){
                                Json::Value item;
                                visit_struct::apply_visitor(ConvertObjectToJson(&item),*pos);
                                arr.append(item);
                        }
                        (*root_)[name]=arr;
                }

        template <typename T>
        void  operator()(const char * name, const T& value)
                {

                        convert_impl(name,value,std::is_base_of<SelfDefinedClass,T>());
                        // (*root_)[name]=value;
                }

        Json::Value * root_;
};
#endif
