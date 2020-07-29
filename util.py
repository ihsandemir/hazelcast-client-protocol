import hashlib
import json
import jsonschema
import os
import re
from enum import Enum

import yaml
from jinja2 import Environment, PackageLoader

from binary import FixedLengthTypes, FixedListTypes, FixedEntryListTypes, FixedMapTypes
from java import java_types_encode, java_types_decode
from cpp import cpp_types_encode, cpp_types_decode, cpp_ignore_service_list, get_size

def java_name(type_name):
    return "".join([capital(part) for part in type_name.replace("(", "").replace(")", "").split("_")])

def cpp_name(type_name):
    return "".join([capital(part) for part in type_name.replace("(", "").replace(")", "").split("_")])

def param_name(type_name):
    return type_name[0].lower() + type_name[1:]


def is_fixed_type(param):
    return param["type"] in FixedLengthTypes


def is_enum(type):
    return type.startswith("Enum_")


def enum_type(lang_name, param_type):
    return lang_name(param_type.split('_', 2)[1])


def capital(txt):
    return txt[0].capitalize() + txt[1:]


def to_upper_snake_case(camel_case_str):
    return re.sub('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))', r'_\1', camel_case_str).upper()
    # s1 = re.sub('(.)([A-Z]+[a-z]+)', r'\1_\2', camel_case_str)
    # return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()


def fixed_params(params):
    return [p for p in params if is_fixed_type(p)]


def var_size_params(params):
    return [p for p in params if not is_fixed_type(p)]


def generate_codecs(services, template, output_dir, env):
    os.makedirs(output_dir, exist_ok=True)
    id_fmt = "0x%02x%02x%02x"
    lang = SupportedLanguages.CPP
    if lang is SupportedLanguages.CPP:
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        cpp_dir = "%s/cpp" % curr_dir
        f = open(os.path.join(cpp_dir, "header_includes.txt"), "r")
        save_file(os.path.join(output_dir, "codecs.h"), f.read(), "w")
        f = open(os.path.join(cpp_dir, "source_header.txt"), "r")
        save_file(os.path.join(output_dir, "codecs.cpp"), f.read(), "w")

    for service in services:
        if service["id"] in language_service_ignore_list[lang]:
            print("[%s] is in ignore list so ignoring it." % service["name"])
            continue
        if "methods" in service:
            methods = service["methods"]
            if methods is None:
                print(type(methods))
            for method in service["methods"]:
                method["request"]["id"] = int(id_fmt % (service["id"], method["id"], 0), 16)
                method["response"]["id"] = int(id_fmt % (service["id"], method["id"], 1), 16)
                events = method.get("events", None)
                if events is not None:
                    for i in range(len(events)):
                        method["events"][i]["id"] = int(id_fmt % (service["id"], method["id"], i + 2), 16)

                codec_file_name = capital(service["name"]) + capital(method["name"]) + 'Codec.' + file_extensions[lang]
                try:
                    if lang is SupportedLanguages.CPP:
                        codec_template = env.get_template("codec-template.h.j2")
                        content = codec_template.render(service_name=service["name"], method=method)
                        save_file(os.path.join(output_dir, "codecs.h"), content, "a+")

                        codec_template = env.get_template("codec-template.cpp.j2")
                        content = codec_template.render(service_name=service["name"], method=method)
                        save_file(os.path.join(output_dir, "codecs.cpp"), content, "a+")
                    else:
                        content = template.render(service_name=service["name"], method=method)
                        save_file(os.path.join(output_dir, codec_file_name), content)
                except NotImplementedError:
                    print("[%s] contains missing type mapping so ignoring it." % codec_file_name)

    f = open(os.path.join(cpp_dir, "footer.txt"), "r")
    content = f.read()
    save_file(os.path.join(output_dir, "codecs.h"), content, "a+")
    save_file(os.path.join(output_dir, "codecs.cpp"), content, "a+")

def generate_custom_codecs(services, template, output_dir, extension, env):
    os.makedirs(output_dir, exist_ok=True)
    if extension is "cpp":
        cpp_header_template = env.get_template("custom-codec-template.h.j2")
        cpp_source_template = env.get_template("custom-codec-template.cpp.j2")
    for service in services:
        if "customTypes" in service:
            custom_types = service["customTypes"]
            for codec in custom_types:
                try:
                    if extension is "cpp":
                        file_name_prefix = codec["name"].lower() + '_codec'
                        header_file_name = file_name_prefix + ".h"
                        source_file_name = file_name_prefix + ".cpp"
                        codec_file_name = header_file_name
                        content = cpp_header_template.render(codec=codec)
                        save_file(os.path.join(output_dir, header_file_name), content)
                        codec_file_name = source_file_name
                        content = cpp_source_template.render(codec=codec)
                        save_file(os.path.join(output_dir, source_file_name), content)
                    else:
                        codec_file_name = capital(codec["name"]) + 'Codec.' + extension
                        content = template.render(codec=codec)
                        save_file(os.path.join(output_dir, codec_file_name), content)
                except NotImplementedError:
                    print("[%s] contains missing type mapping so ignoring it." % codec_file_name)


def item_type(lang_name, param_type):
    if param_type.startswith("List_") or param_type.startswith("ListCN_"):
        return lang_name(param_type.split('_', 1)[1])


def key_type(lang_name, param_type):
    return lang_name(param_type.split('_', 2)[1])


def value_type(lang_name, param_type):
    return lang_name(param_type.split('_', 2)[2])


def is_var_sized_list(param_type):
    return param_type.startswith("List_") and param_type not in FixedListTypes


def is_var_sized_list_contains_nullable(param_type):
    return param_type.startswith("ListCN_") and param_type not in FixedListTypes


def is_var_sized_map(param_type):
    return param_type.startswith("Map_") and param_type not in FixedMapTypes


def is_var_sized_entry_list(param_type):
    return param_type.startswith("EntryList_") and param_type not in FixedEntryListTypes


def load_services(protocol_def_dir):
    service_list = os.listdir(protocol_def_dir)
    services = []
    for service_file in service_list:
        file_path = os.path.join(protocol_def_dir, service_file)
        if os.path.isfile(file_path):
            with open(file_path, 'r') as file:
                data = yaml.load(file, Loader=yaml.Loader)
                services.append(data)
    return services


def validate_services(services, schema_path):
    valid = True
    with open(schema_path, 'r') as schema_file:
        schema = json.load(schema_file)
        for service in services:
            try:
                jsonschema.validate(service, schema)
            except jsonschema.ValidationError as e:
                print("Validation error: %s. schema:%s" % (e.message, list(e.relative_schema_path)))
                valid = False
    return valid


def save_file(file, content, mode="w"):
    m = hashlib.md5()
    m.update(content.encode("utf-8"))
    codec_hash = m.hexdigest()
    with open(file, mode, newline='\n') as file:
        file.writelines(content.replace('!codec_hash!', codec_hash))


class SupportedLanguages(Enum):
    JAVA = 'java'
    CPP = 'cpp'
    # PY = 'py'
    # TS = 'ts'
    # GO = 'go'


output_directories = {
    SupportedLanguages.JAVA: 'hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec/',
    SupportedLanguages.CPP: 'hazelcast/generated-sources/src/hazelcast/client/protocol/codec/',
    # SupportedLanguages.PY: 'hazelcast/protocol/codec/',
    # SupportedLanguages.TS: 'src/codec/',
    # SupportedLanguages.GO: 'internal/proto/'
}

custom_codec_output_directories = {
    SupportedLanguages.JAVA: 'hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec/custom/',
    SupportedLanguages.CPP: 'hazelcast/generated-sources/src/hazelcast/client/protocol/codec/',
    # SupportedLanguages.PY: 'hazelcast/protocol/codec/',
    # SupportedLanguages.TS: 'src/codec/',
    # SupportedLanguages.GO: 'internal/proto/'
}

file_extensions = {
    SupportedLanguages.JAVA: 'java',
    SupportedLanguages.CPP: 'cpp',  # TODO header files ?
    # SupportedLanguages.PY: 'py',
    # SupportedLanguages.TS: 'ts',
    # SupportedLanguages.GO: 'go'
}

language_specific_funcs = {
    'lang_types_encode': {
        SupportedLanguages.JAVA: java_types_encode,
        SupportedLanguages.CPP: cpp_types_encode,
    },
    'lang_types_decode': {
        SupportedLanguages.JAVA: java_types_decode,
        SupportedLanguages.CPP: cpp_types_decode,
    },
    'lang_name': {
        SupportedLanguages.JAVA: java_name,
        SupportedLanguages.CPP: cpp_name,
    },
    'param_name': {
        SupportedLanguages.JAVA: param_name,
        SupportedLanguages.CPP: param_name,
    },
    'escape_keyword': {
        SupportedLanguages.JAVA: lambda x: x,
        SupportedLanguages.CPP: lambda x: x,
    },
}

language_service_ignore_list = {
    SupportedLanguages.JAVA: [],
    SupportedLanguages.CPP: cpp_ignore_service_list,
    # SupportedLanguages.PY: [],
    # SupportedLanguages.TS: [],
    # SupportedLanguages.GO: []
}


def create_environment(lang, namespace):
    env = Environment(loader=PackageLoader(lang.value, '.'))
    env.trim_blocks = True
    env.lstrip_blocks = True
    env.keep_trailing_newline = False
    env.filters["capital"] = capital
    env.globals["to_upper_snake_case"] = to_upper_snake_case
    env.globals["fixed_params"] = fixed_params
    env.globals["var_size_params"] = var_size_params
    env.globals["is_var_sized_list"] = is_var_sized_list
    env.globals["is_var_sized_list_contains_nullable"] = is_var_sized_list_contains_nullable
    env.globals["is_var_sized_entry_list"] = is_var_sized_entry_list
    env.globals["is_var_sized_map"] = is_var_sized_map
    env.globals["is_enum"] = is_enum
    env.globals["item_type"] = item_type
    env.globals["key_type"] = key_type
    env.globals["value_type"] = value_type
    env.globals["enum_type"] = enum_type
    env.globals["lang_types_encode"] = language_specific_funcs['lang_types_encode'][lang]
    env.globals["lang_types_decode"] = language_specific_funcs['lang_types_decode'][lang]
    env.globals["lang_name"] = language_specific_funcs['lang_name'][lang]
    env.globals["namespace"] = namespace
    env.globals["param_name"] = language_specific_funcs['param_name'][lang]
    env.globals["get_size"] = get_size

    return env
