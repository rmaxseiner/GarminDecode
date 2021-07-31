#####################################################################################################
# This program decodes Garmin FIT files and decodes them into individual messages.
# The program decodes individual messages and is able to either create a hierarchical JSON
# message or a flattened mongo database record.  The program decodes FitHeader, FitDefinition,
# FitData and FitCRC messages.
#
# The FIT protocol has many hierarchical components.  MongoDB records are flattened and simplified
# to ease querying and to reduce database size.  If the full detail is needed, create JSON (insert_db = 'full')
# and determine which values should be added.
#
# While most data comes directly from the FIT messages, two values are generated in this
# program.  Activity_id is derived from the FIT file.  Based on the files seen so far, the
# Activity ID is located from position 18 to 28 in the file name with the userid starting the
# record.  If files from a different user are used this will likely need to be amended.
# For example ron@maxseiner.net_12379160600.fit is a standard file name seen.
#             0123456789012345678901234567890
#
# Also, a record_id is added to each message withing an activity.  Record_id starts at 1 for
# an activity and increments by 1 for each message.  This means Activity_id and message_id will
# be unique across all messages created by this program.
#
# The program uses standard libraries except for fitdecode.  This library can be found
# at https://pypi.org/project/fitdecode/.
#
# The program is controlled by a settings.json configuration file.  The following is an example
# setting.  The setting name is passed as a parameter to the this program.
#
# {
# "small": {
#         "directory": "/Users/ronaldmaxseiner/downloads/garminData/DI_CONNECT/DI-Connect-Fitness-Uploaded-Files",
#         "dump_directory": "/Users/ronaldmaxseiner/documents/garmin/dataDecode/debugDump",
#         "fileType": ".fit",
#         "reloadDB": "True",
#         "debug": "False",
#         "db_insert": "db",
#         "document_skip": 50,
#         "document_limit": 25000,
#         "collection_name": "activity_small",
#         "mongo_connection_string":  "localhost"}
# }
# These controls have the following purpose.
# directory - location of the fit data files.
# dump_directory - location to place JSON files when this mode is selected.
# fileType - The extension of the fit data files. Usually ".fit"
# debug - Turns on or off debug messages. Set to "True" to enable debugging otherwise set to "False"
# db_insert - Enables database or JSON output.  Set to "db" for mongo db records. Set to "Full" for JSON.
# document_skip - This features skips records to reduce processing type.  Useful for debugging.
#                   Set to 1 to process all records.  Often set to 100 for debugging.
# collection_name - collection name where MongoDB records will be inserted.
# mongo_db_string - connection string for mongo database.  Use "localhost" for local database
#
###############################################################################################

# import libraries
import argparse
import datetime
import logging
import pprint
from pymongo import MongoClient
import fitdecode
import os
import pandas as pd
import time
import sys
import json
import multiprocessing as mp
import itertools

FULL = 'full'
DB = 'db'


def process_FitDefinitionMessage(FitDefinitionMessage_object):
    """
    This function processes fit definition messages and returns a dict that is able to be converted to JSON
    :param FitDefinitionMessage_object: parsed
    :return:
    """
    FitDefinitionMessage_dict = dict(is_developer_data=FitDefinitionMessage_object.is_developer_data,
                                     local_mesg_num=FitDefinitionMessage_object.local_mesg_num,
                                     time_offset=FitDefinitionMessage_object.time_offset,
                                     mesg_type=process_MessageType(FitDefinitionMessage_object.mesg_type),
                                     global_mesg_num=FitDefinitionMessage_object.global_mesg_num,
                                     endian=FitDefinitionMessage_object.endian,
                                     field_defs=process_FieldDefinition_list(FitDefinitionMessage_object.field_defs),
                                     dev_field_defs=process_devfieldfefinition(FitDefinitionMessage_object.dev_field_defs),
                                     chunk=FitDefinitionMessage_object.chunk)
    return FitDefinitionMessage_dict


def process_FieldDefinition_list(FieldDefinition_list_object, scope=FULL):
    """

    :param FieldDefinition_list_object:
    :return:
    """
    FieldDefinition_list = []
    for FieldDefinition_object in FieldDefinition_list_object:
        FieldDefinition_dict = process_FieldDefinition(FieldDefinition_object, scope)
        FieldDefinition_list.append(FieldDefinition_dict)
    return FieldDefinition_list


def process_FieldDefinition_list_for_db(FieldDefinition_list_object, scope=FULL):
    """
    :param FieldDefinition_list_object:
    :return:
    """
    FieldDefinition_list = {}
    for FieldDefinition_object in FieldDefinition_list_object:
        if isinstance(FieldDefinition_object.type, fitdecode.types.BaseType):
            data_type = FieldDefinition_object.type.name
        elif isinstance(FieldDefinition_object.type, fitdecode.types.FieldType):
            if FieldDefinition_object.type.enum:
                data_type = 'enum'
            else:
                data_type = FieldDefinition_object.type.name
        else:
            data_type = 'other'
        FieldDefinition_list[FieldDefinition_object.name] = data_type
    return FieldDefinition_list


def process_FieldDefinition(FieldDefinition_object, scope=FULL):
    if FieldDefinition_object is None or FieldDefinition_object == 'None':
        FieldDefinition_dict = None
    else:

        if scope == FULL:
            if isinstance(FieldDefinition_object.type, fitdecode.types.BaseType):
                type_message = process_BaseType(FieldDefinition_object.type)
            else:
                type_message = process_FieldType(FieldDefinition_object.type)

            FieldDefinition_dict = dict(base_type=process_BaseType(FieldDefinition_object.base_type),
                                        def_num=FieldDefinition_object.def_num,
                                        field=process_Field(FieldDefinition_object.field),
                                        is_dev=FieldDefinition_object.is_dev,
                                        name=FieldDefinition_object.name,
                                        size=FieldDefinition_object.size,
                                        type=type_message)
        else:
            if isinstance(FieldDefinition_object.type, fitdecode.types.FieldType) and \
                    FieldDefinition_object.field.subfields:
                FieldDefinition_dict = process_SubField_tuple(FieldDefinition_object.field.subfields, DB)
            else:
                FieldDefinition_dict = dict(def_num=FieldDefinition_object.def_num,
                                            name=FieldDefinition_object.name,
                                            size=FieldDefinition_object.size)
    return FieldDefinition_dict


def process_MessageType(MessageType_object, scope = FULL):
    if MessageType_object is None:
        MessageType_dict = None
    else:
        MessageType_dict = dict(name=MessageType_object.name,
                                mesg_num=MessageType_object.mesg_num,
                                fields=process_Field_dict(MessageType_object.fields, scope))
    return MessageType_dict


def process_BaseType(BaseType_object):
    if BaseType_object is None:
        BaseType_dict = None
    else:
        BaseType_dict = dict(fmt=BaseType_object.fmt,
                             identifier=BaseType_object.identifier,
                             name=BaseType_object.name,
                             size=BaseType_object.size,
                             type_num=BaseType_object.type_num)
    return BaseType_dict


def process_Field_dict(Field_dict_object, scope=FULL):
    if Field_dict_object is None:
        Field_dict_dict = None
    elif isinstance(Field_dict_object, dict):
        Field_dict_dict = []
        for Field_object_index in Field_dict_object:
            Field_dict_dict.append(Field_dict_object[Field_object_index])
    else:
        logging.error("process_Field_dict(): Unhandled data type %s while processing fields collection",
                      type(Field_dict_object))
        Field_dict_dict = 'Additional processing required'

    return Field_dict_dict


def process_Field(Field_object, scope=FULL):
    if Field_object is None:
        Field_dict = None
    else:
        type_message = {}
        if Field_object.is_base_type:
            type_message = process_BaseType(Field_object.type)
        else:
            type_message = process_FieldType(Field_object.type)

        Field_dict = dict(name=Field_object.name,
                          type=type_message,
                          field_type=Field_object.field_type,
                          scale=Field_object.scale,
                          offset=Field_object.offset,
                          units=Field_object.units,
                          components=process_ComponentField_tuple(Field_object.components),
                          subfields=process_SubField_tuple(Field_object.subfields, scope),
                          def_num=Field_object.def_num,
                          base_type=process_BaseType(Field_object.base_type))
    return Field_dict


def process_ComponentField_tuple(ComponentField_tuple):

    if ComponentField_tuple is None:
        ComponentField_tuple_dict = None
    else:
        ComponentField_tuple_dict = []
        for ComponentField_object in ComponentField_tuple:
            ComponentField_tuple_dict.append(process_ComponentField(ComponentField_object))
    return ComponentField_tuple_dict


def process_ComponentField(ComponentField_object):
    if ComponentField_object is None or ComponentField_object == 'None':
        ComponentField_dict = None
    else:
        ComponentField_dict = dict(name=ComponentField_object.name,
                                   bit_offset=ComponentField_object.bit_offset,
                                   bits=ComponentField_object.bits,
                                   def_num=ComponentField_object.def_num,
                                   field_type=ComponentField_object.field_type,
                                   offset=ComponentField_object.offset,
                                   scale=ComponentField_object.scale,
                                   units=ComponentField_object.units)
    return ComponentField_dict


def process_SubField_tuple(SubField_tuple, scope):
    if SubField_tuple is None:
        SubFields_tuple_dict = None
    else:
        SubFields_tuple_dict = []
        for SubField_object in SubField_tuple:
            SubFields_tuple_dict.append(process_SubField(SubField_object, scope))
    return SubFields_tuple_dict


def process_SubField(SubField_object, scope):
    if SubField_object is None:
        SubField_dict = None
    else:
        type_dict = {}
        if SubField_object.is_base_type:
            type_dict = process_BaseType(SubField_object.type)
        else:
            type_dict = process_FieldType(SubField_object.type)

        if scope == FULL:
            SubField_dict = dict(name=SubField_object.name,
                                 type=type_dict,
                                 field_type=SubField_object.field_type,
                                 scale=SubField_object.scale,
                                 offset=SubField_object.offset,
                                 units=SubField_object.units,
                                 components=process_ComponentField_tuple(SubField_object.components),
                                 def_num=SubField_object.def_num,
                                 base_type=process_BaseType(SubField_object.base_type),
                                 ref_fields=process_ReferenceField_tuple(SubField_object.ref_fields))
        else:
            SubField_dict = dict(name=SubField_object.name,
                                 field_type=SubField_object.field_type,
                                 def_num=SubField_object.def_num)

    return SubField_dict


def process_ReferenceField_tuple(ReferenceField_tuple):
    ref_fields_dict = []
    for ref_field_object in ReferenceField_tuple:
        if ref_field_object is None:
            ref_field_dict = None
        elif isinstance(ref_field_object, fitdecode.types.ReferenceField):
            ref_field_dict = dict(def_num=ref_field_object.def_num,
                                  name=ref_field_object.name,
                                  raw_value=ref_field_object.raw_value,
                                  value=process_value(ref_field_object.value, ref_field_object.name))
            ref_fields_dict.append(ref_field_dict)
        else:
            logging.error("process_ref_fields_message(): Unhandled data type %s while processing fields collection",
                          type(ref_field_object))
            ref_field_dict = 'Additional processing required'
    return ref_fields_dict


def process_FieldType(FieldType_object):
    if FieldType_object is None:
        FieldType_message = None
    else:
        if isinstance(FieldType_object.enum, dict):
            FieldType_message = dict(name=FieldType_object.name,
                                     baseType=process_BaseType(FieldType_object.base_type),
                                     enum=process_enum(FieldType_object.enum))
        else:
            FieldType_message = dict(name=FieldType_object.name,
                                     baseType=process_BaseType(FieldType_object.base_type))
    return FieldType_message


def process_enum(enum_dict):
    mongo_safe_dict = {}

    for key in enum_dict:
        mongo_safe_dict[str(key)] = enum_dict[key]

    return mongo_safe_dict


def process_FieldData_list_for_db(FieldData_list, common):
    scope = DB
    if isinstance(FieldData_list, tuple):
        logging.error("process_fields_data_message(): Processing list but found tuple")
        FieldsData_list_dict = 'Additional processing required'

    elif FieldData_list is None:
        FieldsData_list_dict = None

    elif isinstance(FieldData_list, list):
        FieldsData_list_dict = {}
        for field_data_index in FieldData_list:
            if isinstance(field_data_index.value, tuple):
                counter = 1
                for value in field_data_index.value:
                    FieldsData_list_dict[field_data_index.name +
                                         '_' + str(counter)] = process_value(value,
                                                                             field_data_index.name + '_' + str(counter))
                    counter = counter + 1
            else:
                FieldsData_list_dict[field_data_index.name] = process_value(field_data_index.value, field_data_index.name)

    elif isinstance(FieldData_list, dict):
        FieldsData_list_dict = {}
        for field_data_index in FieldData_list:
            FieldsData_list_dict[FieldData_list[field_data_index].name] = process_value(
                FieldData_list[field_data_index].value,
                FieldData_list[field_data_index].name)

    else:
        logging.error("process_fields_data_message(): Unhandled data type %s while processing fields collection", type(FieldData_list))
        FieldsData_list_dict = 'Additional processing required'
    return FieldsData_list_dict


def process_FieldData_list_for_full(FieldData_list, common):
    if isinstance(FieldData_list, tuple):
        logging.error("process_fields_data_message(): Processing list but found tuple")
        FieldsData_list_dict = 'Additional processing required'

    elif FieldData_list is None:
        FieldsData_list_dict = None

    elif isinstance(FieldData_list, list):
        FieldsData_list_dict = []
        for field_data_index in FieldData_list:
            FieldsData_list_dict.append(process_FieldData_for_full(field_data_index))
    elif isinstance(FieldData_list, dict):
        FieldsData_list_dict = []
        for field_data_index in FieldData_list:
            FieldsData_list_dict.append(process_FieldData_for_full(FieldData_list[field_data_index]))
    else:
        logging.error("process_fields_data_message(): Unhandled data type %s while processing fields collection", type(FieldData_list))
        FieldsData_list_dict = 'Additional processing required'
    return FieldsData_list_dict


def process_FieldData_for_full(field_data_object):

    if field_data_object is None:
        FieldData_dict = None
    else:
        type_message = {}
        if field_data_object.is_base_type:
            type_message = process_BaseType(field_data_object.type)
        else:
            type_message = process_FieldType(field_data_object.type)

        if isinstance(field_data_object.field, fitdecode.types.Field):
            field_message = process_Field(field_data_object.field)
        elif isinstance(field_data_object.field, fitdecode.types.SubField):
            field_message = process_SubField(field_data_object.field, FULL)
        elif field_data_object.field is None:
            field_message = None
        else:
            logging.error("process_field_data_message(): Unhandled data type %s while processing fields collection", type(field_data_object.field))
            field_message = 'Additional processing required'

        FieldData_dict = dict(def_num=field_data_object.def_num,
                              name=field_data_object.name,
                              units=field_data_object.units,
                              value=process_value(field_data_object.value, field_data_object.name))
    return FieldData_dict


def process_value(value_object, name='Empty'):

    if isinstance(value_object, datetime.date):
        value_string = value_object.strftime("%Y-%m-%d %H:%M:%S %z")
    elif isinstance(value_object, datetime.time):
        value_string = value_object.strftime("%H:%M:%S")
    elif name == 'position_lat' or name == 'position_long':
        if value_object is None:
            value_string = 'None'
        else:
            value_string = value_object / 11930465

    else:
        value_string = value_object
    return value_string


def process_devfieldfefinition(DevFieldDefinition_object):
    """
    This function processes DevFieldDefinition object.  No files I have actually contain development fields and
    therefore I have not completely implemented this object
    :param DevFieldDefinition_object:
    :return: dict - to be converted to JSON
    """
    if DevFieldDefinition_object is None:
        dev_field_definitions_dict = None
    elif isinstance(DevFieldDefinition_object, list):
        if len(DevFieldDefinition_object) == 0:
            dev_field_definitions_dict = None
        else:
            logging.error(
                'process_dev_fields_message(): List with values was found. ' +
                'Search for \'Additional processing required\' in output')
            dev_field_definitions_dict = 'Additional processing required'
    else:
        logging.error(
            'process_dev_fields_message(): Unhandled data type dev_field_definition is defined. ' +
            'Search for \'Additional processing required\' in output')
        dev_field_definitions_dict = 'Additional processing required'
    return dev_field_definitions_dict


def connect_to_mongo(settings):
    # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
    client = MongoClient(settings['mongo_connection_string'])
    db = client.fit
    # Issue the serverStatus command and print the results
    server_status_result = db.command("serverStatus")
    if settings['debug']:
        pp = pprint.PrettyPrinter(indent=3)
        pp.pprint(server_status_result)
        del pp
    return db


def reset_db(settings, db):
    if settings['reloadDB']:
        db[settings['collection_name']].delete_many({})
    return


def configure_logging():
    logging.basicConfig(filename='src/output.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')


def get_settings(configuration_set):
    with open('settings.json', 'r') as f:
        config = json.load(f)

    settings = config[configuration_set]

    for key, value in settings.items():
        if value == 'True':
            settings[key] = True
        if value == 'False':
            settings[key] = False

    return settings


def process_fit_data(frame, scope):
    # Here, frame is a FitDataMessage object.
    # A FitDataMessage object contains decoded values that
    # are directly usable in your script logic.

    common = dict(type='FitDataMessage',
                       name=frame.name)
    # process_FieldData_list(frame.fields, common, db, settings, DB)

    full_activity = dict(message_type='FitDataMessage',
                         chunk=frame.chunk,
                         defMessage=process_FitDefinitionMessage(frame.def_mesg),
                         fields=process_FieldData_list_for_full(frame.fields, common),
                         frame_type=frame.frame_type,
                         global_mesg_num=frame.global_mesg_num,
                         isDeveloperData=frame.is_developer_data,
                         local_mesg_num=frame.local_mesg_num,
                         name=frame.name,
                         timeOffset=frame.time_offset)

    db_activity = dict(message_type='FitDataMessage',
                         message_chunk=frame.chunk,
                         message_frame_type=frame.frame_type,
                         message_global_mesg_num=frame.global_mesg_num,
                         message_isDeveloperData=frame.is_developer_data,
                         message_local_mesg_num=frame.local_mesg_num,
                         message_name=frame.name,
                         message_timeOffset=frame.time_offset)
    db_activity.update(process_FieldData_list_for_db(frame.fields, common))

    return db_activity, full_activity


def process_fit_definition(frame, scope):
    # Here, frame is a FitDataMessage object.
    # A FitDataMessage object contains decoded values that
    # are directly usable in your script logic.

    full_activity = dict(message_type='FitDefinitionMessage',
                         all_field_defs=process_FieldDefinition_list(frame.all_field_defs),
                         chunk=frame.chunk,
                         dev_field_def=process_devfieldfefinition(frame.dev_field_defs),
                         endian=frame.endian,
                         field_defs=process_FieldDefinition_list(frame.field_defs),
                         frame_type=frame.frame_type,
                         global_mesg_num=frame.global_mesg_num,
                         isDeveloperData=frame.is_developer_data,
                         local_mesg_num=frame.local_mesg_num,
                         mesg_type=process_MessageType(frame.mesg_type, FULL),
                         timeOffset=frame.time_offset,
                         name=frame.name,
                         time_offset=frame.time_offset)

    db_activity = dict(message_type='FitDefinitionMessage',
                         # chunk=frame.chunk,
                         # endian=frame.endian,
                         # field_defs=process_FieldDefinition_list_for_db(frame.field_defs),
                         message_frame_type=frame.frame_type,
                         message_global_mesg_num=frame.global_mesg_num,
                         # isDeveloperData=frame.is_developer_data,
                         message_local_mesg_num=frame.local_mesg_num,
                         # mesg_type=process_MessageType(frame.mesg_type, FULL),
                         message_name=frame.name,
                         message_time_offset=frame.time_offset)
    db_activity.update(process_FieldDefinition_list_for_db(frame.field_defs))
    return db_activity, full_activity


def process_fit_header(frame, scope):

    full_activity = dict(message_type='FitHeader',
                         message_size=frame.header_size,
                         message_protoVersion=frame.proto_ver,
                         message_profileVersion=frame.profile_ver,
                         message_bodySize=frame.body_size,
                         message_header_size=frame.header_size,
                         message_crc=frame.crc,
                         message_crcMatched=frame.crc_matched,
                         message_chunk=frame.chunk)

    return full_activity, full_activity


def process_fit_crc(frame):
    db_activity = db_activity = dict(message_type='FitCRC',
                                     message_frame_type=frame.frame_type,
                                     matched=frame.matched
                                     )

    full_activity = dict(message_type='FitCRC',
                         chunk=frame.chunk,
                         crc=frame.crc,
                         frame_type=frame.frame_type,
                         matched=frame.matched)

    return db_activity, full_activity


def extract_activity_id_from_file_name(file_name):
    activity_id = file_name[18:29]

    if not activity_id.isnumeric():
        logging.error('Potential parse error on activity id.  Parsed value is %s', activity_id)

    return activity_id


def load_fit_collection_into_data_frame(collection):
    pp = pprint.PrettyPrinter(indent=3)
    cycling_activities = collection.find({'message_global_mesg_num': 20})

    data_frame = pd.DataFrame(list(cycling_activities))

    return data_frame


def get_configuration_set_from_command_line_args(command_line_args):
    parser = argparse.ArgumentParser(description='Process Garmin FIT files')
    parser.add_argument('-c', nargs=1, required=True, type=ascii,
                        help='The command line set that should be selected from settings.json')

    args = parser.parse_args()
    configuration_set = args.c[0][1:len(args.c[0])-1]
    return configuration_set


def process_fit_file(file):
    activity_id = extract_activity_id_from_file_name(file)
    settings = get_settings('full')
    db = connect_to_mongo(settings)

    record_id = 0
    with fitdecode.FitReader(settings["directory"] + '/' + file) as fit:

        for frame in fit:
            record_id += 1
            db_activity = {}
            full_activity = {}

            if isinstance(frame, fitdecode.FitDataMessage):
                db_activity, full_activity = process_fit_data(frame, settings['db_insert'])

            elif isinstance(frame, fitdecode.FitDefinitionMessage):
                db_activity, full_activity = process_fit_definition(frame, settings['db_insert'])

            elif isinstance(frame, fitdecode.FitHeader):
                db_activity, full_activity = process_fit_header(frame, settings['db_insert'])

            elif isinstance(frame, fitdecode.FitCRC):
                db_activity, full_activity = process_fit_crc(frame)

            else:
                logging.error("main(): Unhandled frame type ", frame.frame_type)
                db_activity = dict(message_type='Unknown')
                full_activity = dict(message_type='Unknown')

            db_activity.update(dict(record_id=record_id))
            full_activity.update(dict(record_id=record_id))
            db_activity.update(dict(activity_id=activity_id))
            full_activity.update(dict(activity_id=activity_id))

            if settings['debug']:

                dump_file = open(settings['dump_directory'] + "/" +
                                 "dump_file_" + activity_id + str(record_id) + ".txt", 'w')
                pp = pprint.PrettyPrinter(indent=3, stream=dump_file)
                pp.pprint(full_activity)
                dump_file.close()
                del pp
                print(record_id)

            if settings['db_insert'] == DB:
                if db_activity:
                    db[settings['collection_name']].insert_one(db_activity)
            elif settings['db_insert'] == FULL:
                db[settings['collection_name']].insert_one(full_activity)
            else:
                logging.error("Unknown db_insert_setting %s", settings['db_insert'])
    print('Processed File', file)
    return


def main(command_line_args):
    configure_logging()
    configuration_set = get_configuration_set_from_command_line_args(command_line_args)
    settings = get_settings(configuration_set)
    db = connect_to_mongo(settings)
    reset_db(settings, db)

    files = os.listdir(settings["directory"])
    if 'activity_ids' in settings:
        fit_files = [file for file in settings['activity_ids'] if file[-4:].lower() == settings["fileType"]]
    else:
        fit_files = [file for file in files if file[-4:].lower() == settings["fileType"]]

    fit_file_count = 0

    start_time = time.time()
    with mp.Pool(32) as pool:
        results = pool.map(process_fit_file, fit_files, chunksize=100)

    # for file in fit_files:
    #     fit_file_count += 1
    #     if fit_file_count >= settings['document_limit']:
    #         break
    #     if fit_file_count % settings['document_skip'] != 0 and not 'activity_ids' in settings:
    #         continue
    #     print('file count = ', fit_file_count)
    #     process_fit_file(file, settings, scope, db)
    end_time = time.time()

    duration = end_time - start_time

    print('The total duration is ', str(duration))


if __name__ == '__main__':
    main(sys.argv[1:])
