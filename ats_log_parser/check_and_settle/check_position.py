# -*- coding: utf-8 -*-
import os
import logging
import pandas as pd
import xml.etree.ElementTree as ElementTree


# -------------------------------------------------------------------------------------------------------------------
def check_position(settle_info):
    from ..log_analyst.settle_process import __get_position_csv_name

    position_csv_path = __get_position_csv_name(settle_info)
    strategy_positions = parse_strategy_config(settle_info)
    csv_positions = parse_position_csv(position_csv_path)

    if csv_positions == strategy_positions:
        logging.info("mysql_positions == strategy_positions")
        return True
    else:
        logging.info('strategy_positions')
        for i in strategy_positions:
            logging.info(str(i))
        logging.info('holding_csv_position')
        for i in csv_positions:
            logging.info(str(i))
        logging.info("mysql_positions != strategy_positions")
        return False


# ------------------------------------------------------------------------------------------------------------------
def parse_strategy_config(settle_info):
    files = os.listdir(settle_info.strategy_config_folder)
    files_path_list = []
    for file in files:
        files_path_list.append(os.path.join(settle_info.strategy_config_folder, file))

    strategy_positions = []
    for file_path in files_path_list:
        # path, file = os.path.split(file_path)
        # filename, ext = os.path.splitext(file)
        tree = ElementTree.parse(file_path)
        root = tree.getroot()
        # 得到symbol tree内的所有信息
        strategy_name = ''
        for level1 in root:
            if level1.tag == "name":
                strategy_name = level1.text
            if level1.tag == "position":
                for position in level1:
                    if position.tag == "long":
                        for long in position:
                            strategy_position = dict()
                            strategy_position['name'] = strategy_name
                            strategy_position['contract'] = long.get("instrument")
                            strategy_position['direction'] = 1
                            strategy_position['quantity'] = int(long.get("quantity"))
                            strategy_positions.append(strategy_position)
                    elif position.tag == "short":
                        for short in position:
                            strategy_position = dict()
                            strategy_position['name'] = strategy_name
                            strategy_position['contract'] = short.get("instrument")
                            strategy_position['direction'] = -1
                            strategy_position['quantity'] = int(short.get("quantity"))
                            strategy_positions.append(strategy_position)

    strategy_positions.sort(key=lambda x: x['name'])
    return strategy_positions


# ----------------------------------------------------------------------------------------------------------------------
def parse_position_csv(position_csv_path):
    csv_position = pd.read_csv(position_csv_path,
                               na_values=['nan'],
                               keep_default_na=False)
    csv_position = csv_position.to_dict('records')
    combined_position = []
    name_list = []
    for item in csv_position:
        if (item['open_strategy'] not in name_list) and \
                (item['close_strategy'] == ''):
            position = dict()
            position['name'] = item['open_strategy']
            position['contract'] = item['contract']
            position['direction'] = int(item['dir'])
            position['quantity'] = int(item['quantity'])
            combined_position.append(position)
            name_list.append(item['open_strategy'])
        else:
            for value in combined_position:
                if (value['name'] == item['open_strategy']) and \
                        (value['direction'] == int(item['dir'])):
                    value['quantity'] += item['quantity']

    combined_position.sort(key=lambda x: x['name'])
    return combined_position


def change_xml_setting(filename):
    xml_path = os.path.join(r'\\172.18.93.131\users\hongxing\ly_duichong_3_300_qxy\strategy_config', filename)
    output_path = os.path.join(r'\\172.18.93.131\users\hongxing\ly_duichong_3_300_qxy\out', filename)
    tree = ElementTree.parse(xml_path)
    user = tree.findall('parameters/user')[0]
    for param in user:
        if param.get('name') == 'open_fund':
            param.set('value', '1')

    positions = tree.findall('position')[0]
    for position in positions:
        for ls in position:
            if ls.get('quantity'):
                ls.set('quantity', "1")
    tree.write(output_path, encoding="UTF-8", xml_declaration=True)


if __name__ == "__main__":
    file_list = os.listdir(r'\\172.18.93.131\users\hongxing\ly_duichong_3_300_qxy\strategy_config')
    for filename in file_list:
        change_xml_setting(filename)
        print(filename)

