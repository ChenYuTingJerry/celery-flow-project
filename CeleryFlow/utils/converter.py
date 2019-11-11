import re


class StringConvert:
    @staticmethod
    def to_snake_case(text):
        str1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
        return re.sub('([a-z])([A-Z])', r'\1_\2', str1).lower()

    @staticmethod
    def to_lower_camel_case(text):
        components = text.split('_')
        return components[0] + ''.join(x.capitalize() for x in components[1:])

    @staticmethod
    def to_upper_camel_case(text):
        components = text.split('_')
        return ''.join(x.capitalize() for x in components)


class DictConvert:
    @staticmethod
    def to_snake_case(dict_obj):
        if isinstance(dict_obj, dict):
            item = {}
            for key, value in dict_obj.items():
                if isinstance(value, dict):
                    item[StringConvert.to_snake_case(key)] = DictConvert.to_snake_case(value)
                else:
                    item[StringConvert.to_snake_case(key)] = value
            return item
        return dict_obj

    @staticmethod
    def to_upper_camel_case(dict_obj):
        if isinstance(dict_obj, dict):
            item = {}
            for key, value in dict_obj.items():
                if isinstance(value, dict):
                    item[StringConvert.to_upper_camel_case(key)] = DictConvert.to_upper_camel_case(value)
                else:
                    item[StringConvert.to_upper_camel_case(key)] = value
            return item
        return dict_obj

    @staticmethod
    def to_lower_camel_case(dict_obj):
        if isinstance(dict_obj, dict):
            item = {}
            for key, value in dict_obj.items():
                if isinstance(value, dict):
                    item[StringConvert.to_lower_camel_case(key)] = DictConvert.to_lower_camel_case(value)
                else:
                    item[StringConvert.to_lower_camel_case(key)] = value
            return item
        return dict_obj
