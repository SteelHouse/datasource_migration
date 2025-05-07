from configparser import ConfigParser


def config(filename, section):
    parser = ConfigParser()
    parser.read(filename)
    configuration = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            configuration[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return configuration


def load_config_with_pass(config_file, pass_file, section):
    base_config = config(config_file, section)
    pass_config = config(pass_file, section)
    base_config['password'] = pass_config['password']
    return base_config
