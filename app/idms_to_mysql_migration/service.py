import logging
import re
from flask import request
from os import path
from typing import Optional, List, Tuple

from app.base_migration_service import BaseMigrationService
from app.config import S3_EVE_BUCKET, S3_THEORY_BUCKET
from app.cli import log
from app.constants.idms import IDMS_ELEM_ITEM_REGEX, IDMS_RECORD_NAME_REGEX, IDMS_STD_PIC_W_LEN_REGEX, \
    IDMS_SET_HEADER_REGEX, \
    IDMS_SET_OWNER_REGEX, IDMS_SET_MEMBER_REGEX, IDMS_SET_MEMBER_KEY_REGEX, IDMS_ITEM_REGEX, \
    IDMS_DECIMAL_PIC_W_LEN_REGEX, IDMS_SIGNED_INT_PIC_W_LEN_REGEX, IDMS_DECIMAL_PIC_W_FIRST_LEN_REGEX
from app.idms_to_mysql_migration.mysql_column import MySQLColumn
from app.idms_to_mysql_migration.constants import IDMS_TO_MYSQL_TYPE_MAP, MYSQL_ID_COLUMN
from app.idms_to_mysql_migration.mysql_table import MySQLTable
from app.utils.idms import IDMSUtils


class IDMSToMySQLMigrationService(BaseMigrationService):
    def __init__(self):
        super().__init__()
        self.mysql_out_file = None
        self.mysql_tables: List[MySQLTable] = list()
        self.cobol_out_file = None
        self.cobol_out_file_paths: List[str] = list()

        # Request data
        self.s3_schemas_path = ''
        self.s3_data_path = ''
        self.s3_sets_path = ''
        self.s3_out_path = ''
        self.should_upload_to_s3 = True
        self.schemas_suffix = ''
        self.data_suffix = ''
        self.set_suffix = ''
        self.should_migrate_fks = False
        self.cobol_copybook_ext = ''
        self.encoding = ''

    def migrate(self) -> dict:
        """
        Migrate all IDMS records (tables) to new MySQL tables.
        Includes schemas and data.

        :return: Output file path in S3.
        """

        self.start_job()

        # Reinitialize state
        self.mysql_out_file = None
        self.mysql_tables = list()
        self.cobol_out_file = None
        self.cobol_out_file_paths = list()

        # Create and open output files
        mysql_out_filename = 'idms_migration.sql'
        mysql_out_file_path = path.join(self.temp_out_dir, mysql_out_filename)
        self.mysql_out_file = open(mysql_out_file_path, 'a')

        # Parse request data
        data = request.json
        base_path = data['base_path']
        self.s3_schemas_path = f"inputs/{base_path}/schemas"
        self.s3_data_path = f"inputs/{base_path}/data"
        self.s3_sets_path = f"inputs/{base_path}/sets"
        self.s3_out_path = f"outputs/{base_path}/{mysql_out_filename}"
        s3_cobol_copybook_out_path = f'inputs/{data["cobol_copybook_out_path"]}'

        should_upload_to_s3_key = 'upload_to_s3'
        self.should_upload_to_s3 = data[should_upload_to_s3_key] if should_upload_to_s3_key in data.keys() else True

        schemas_suffix_key = 'schemas_suffix'
        self.schemas_suffix = data[schemas_suffix_key] if schemas_suffix_key in data.keys() else '_SCHEMA.txt'

        data_suffix_key = 'data_suffix'
        self.data_suffix = data[data_suffix_key] if data_suffix_key in data.keys() else '_DATA.txt'

        set_suffix_key = 'set_suffix'
        self.set_suffix = data[set_suffix_key] if set_suffix_key in data.keys() else '.txt'

        should_migrate_fks_key = 'migrate_fks'
        self.should_migrate_fks = data[should_migrate_fks_key] if should_migrate_fks_key in data.keys() else False

        cobol_copybook_ext_key = 'cobol_copybook_ext'
        self.cobol_copybook_ext = data[cobol_copybook_ext_key] if cobol_copybook_ext_key in data.keys() else ''

        encoding_key = 'encoding'
        self.encoding = data[encoding_key] if encoding_key in data.keys() else 'utf-8'

        # List IDMS schemas in S3
        schema_objects = self.bucket.objects.filter(Prefix=self.s3_schemas_path)
        for schema_obj in schema_objects:
            # Download IDMS schema from S3
            schema_filename = path.basename(schema_obj.key)

            if schema_filename.strip() == '':
                continue

            log(f'{self.tag}Downloading {schema_obj.key}...', level=logging.DEBUG)
            local_schema_path = path.join(self.temp_inp_dir, schema_filename)
            self.bucket.download_file(schema_obj.key, local_schema_path)

            # Create COBOL copybook output file
            schema_name = schema_filename.replace(self.schemas_suffix, "")
            cobol_out_filename = f'{schema_name}.txt'
            cobol_out_file_path = path.join(self.temp_out_dir, cobol_out_filename)
            self.cobol_out_file_paths.append(cobol_out_file_path)
            self.cobol_out_file = open(cobol_out_file_path, 'a')

            # Write main group item to copybook file
            self.cobol_out_file.write(f'{" " * 7}01 {schema_name}.\n')

            # Migrate IDMS schema file to a new MySQL table
            log(f'{self.tag}Migrating schema from {schema_obj.key}...', level=logging.DEBUG)
            mysql_table = self.__migrate_schema(local_schema_path)

            # Close COBOL copybook output file
            self.cobol_out_file.close()

            # Download IDMS data from S3
            data_filename = schema_filename.replace(self.schemas_suffix, self.data_suffix, 1)
            data_key = f'{self.s3_data_path}/{data_filename}'
            local_data_path = path.join(self.temp_inp_dir, data_filename)
            has_data = False
            log(f'{self.tag}Downloading {data_key}...', level=logging.DEBUG)
            try:
                self.bucket.download_file(data_key, local_data_path)
                has_data = True
            except:
                log(f'No data found for IDMS schema "{schema_obj.key}".', level=logging.WARNING)

            if has_data:
                # Migrate IDMS data file to rows for the newly-created MySQL table
                log(f'{self.tag}Migrating data from {data_key}...', level=logging.DEBUG)
                self.__migrate_data(local_data_path, mysql_table)

            # TODO: Delete all local downloaded files from "temp/inputs"

        # List IDMS sets in S3
        set_objects = self.bucket.objects.filter(Prefix=self.s3_sets_path)
        for set_obj in set_objects:
            # Download IDMS set from S3
            set_filename = path.basename(set_obj.key)

            if set_filename.strip() == '':
                continue

            local_set_path = path.join(self.temp_inp_dir, set_filename)
            log(f'{self.tag}Downloading {set_obj.key}...', level=logging.DEBUG)
            self.bucket.download_file(set_obj.key, local_set_path)

            # Migrate IDMS set to MySQL foreign key constraints or view
            log(f'{self.tag}Migrating set from {set_obj.key}...', level=logging.DEBUG)
            self.__migrate_set(local_set_path)

        self.mysql_out_file.close()

        s3_copybooks_paths = list()

        if self.should_upload_to_s3:
            # Upload MySQL output file to S3
            log(f'{self.tag}Uploading output files to S3...', level=logging.DEBUG)
            self.bucket.upload_file(mysql_out_file_path, self.s3_out_path)

            # Upload COBOL copybooks to S3
            theory_bucket = self.s3.Bucket(S3_THEORY_BUCKET)
            for copybook_path in self.cobol_out_file_paths:
                s3_copybook_path = f'{s3_cobol_copybook_out_path}/{path.basename(copybook_path)}'
                s3_copybooks_paths.append(s3_copybook_path)
                theory_bucket.upload_file(copybook_path, s3_copybook_path)

        self.succeed()

        return {
            'eve_bucket': S3_EVE_BUCKET,
            'theory_bucket': S3_THEORY_BUCKET,
            'sql_file_path': self.s3_out_path,
            'copybook_paths': s3_copybooks_paths,
        }

    def __migrate_schema(self, file_path: str) -> MySQLTable:
        """
        Migrate IDMS schema file to a new MySQL table.

        :param file_path: IDMS schema file path.
        :return: MySQL table object.
        """

        file_contents = open(file_path).read()

        # Create MySQL table object
        idms_record_name_match = re.search(IDMS_RECORD_NAME_REGEX, file_contents)

        if idms_record_name_match is None:
            raise Exception('Failed to find IDMS record name within schema file.')

        mysql_table_name = idms_record_name_match.group('name')
        mysql_table_name = IDMSUtils.name_to_snake_case(mysql_table_name)
        create_stmt = f'CREATE TABLE {mysql_table_name}(\n' + \
                      "\tid CHAR(9) NOT NULL DEFAULT '',\n"
        mysql_table = MySQLTable(mysql_table_name, columns=[MYSQL_ID_COLUMN])

        # Migrate schema to MySQL columns
        for line in file_contents.splitlines():
            match = re.match(IDMS_ELEM_ITEM_REGEX, line.strip())

            if match is None:
                continue

            # Skip conditions (level 88 items)
            if match.group('lvl') == '88':
                continue

            # Create row from each match
            mysql_col_def, cobol_pic = self.__migrate_schema_item(match)

            if mysql_col_def is not None:
                create_stmt += f'\t{mysql_col_def},\n'
                mysql_table.add_column(cobol_pic)

        # Create COBOL copybook from IDMS schema
        # TODO: Add support for condition items (88 level)
        for line in file_contents.splitlines():
            match = re.match(IDMS_ITEM_REGEX, line.strip())

            if match is None:
                continue

            self.__create_cobol_pic_item(match)

        # Save MySQL table object
        self.mysql_tables.append(mysql_table)

        # Add primary key definition and closing bracket for "CREATE TABLE" statement
        create_stmt += '\tPRIMARY KEY (id)\n' + \
                       ');\n'

        # Write to output file
        self.mysql_out_file.write(create_stmt)

        return mysql_table

    def __migrate_schema_item(self, match) -> Tuple[Optional[str], Optional[MySQLColumn]]:
        """
        Migrate IDMS schema item to a new MySQL column.

        :param match: Regex match.
        :return: Tuple of:
            - MySQL column definition for creating the column. Intended for use inside of a "CREATE TABLE" statement.
            - MySQL column object.
        """

        # Get name
        name = match.group('name')

        if name == 'FILLER':
            return None, None

        name = IDMSUtils.name_to_snake_case(name)

        # Get type
        idms_pic = match.group('type')
        std_pic_w_len_match = re.match(IDMS_STD_PIC_W_LEN_REGEX, idms_pic)
        signed_int_w_len_match = re.match(IDMS_SIGNED_INT_PIC_W_LEN_REGEX, idms_pic)
        decimal_pic_w_len_match = re.match(IDMS_DECIMAL_PIC_W_LEN_REGEX, idms_pic)
        decimal_pic_w_first_len_match = re.match(IDMS_DECIMAL_PIC_W_FIRST_LEN_REGEX, idms_pic)
        len_1: Optional[int] = None
        len_2: Optional[int] = None

        if std_pic_w_len_match is not None:
            # PIC in format "X(1)"
            var_type = IDMS_TO_MYSQL_TYPE_MAP[std_pic_w_len_match.group('type')]
            var_len = int(std_pic_w_len_match.group("len"))
            var_len_str = '' if var_type == 'NUMERIC' else f'({var_len})'
        elif signed_int_w_len_match is not None:
            # PIC in format "S9(1)"
            var_type = 'BIGINT'
            var_len = int(signed_int_w_len_match.group('len').lstrip('0'))
            var_len_str = f'({var_len})'
        elif decimal_pic_w_len_match is not None:
            # PIC in format "S9(1)V9(1)"
            var_type = 'DECIMAL'
            len_1 = int(decimal_pic_w_len_match.group('len_1').lstrip('0'))
            len_2 = int(decimal_pic_w_len_match.group('len_2').lstrip('0'))
            var_len = len_1 + len_2
            var_len_str = f'({len_1},{len_2})'
        elif decimal_pic_w_first_len_match is not None:
            # PIC in format "S9(1)V99"
            var_type = 'DECIMAL'
            len_1 = int(decimal_pic_w_first_len_match.group('len_1').lstrip('0'))
            len_2 = len(decimal_pic_w_first_len_match.group('len_2'))
            var_len = len_1 + len_2
            var_len_str = f'({len_1},{len_2})'
        else:
            # PIC in format "XX"
            var_type = IDMS_TO_MYSQL_TYPE_MAP[idms_pic[0]]
            var_len = len(idms_pic)
            var_len_str = '' if var_type == 'NUMERIC' else f'({var_len})'

        # Get default value
        default_val = match.group('def_val')

        if default_val:
            if default_val.startswith('SPACE'):
                # Default value for "SPACE" is actually an empty string, since the padding is automatically added via
                # MySQL's "CHAR" type.
                default_val = " DEFAULT ''"
            elif default_val.startswith('ZERO'):
                default_val = ' DEFAULT 0'
            else:
                default_val = f' DEFAULT {default_val}'
        else:
            default_val = ''

        # Save parsed data
        column = MySQLColumn(
            name=name,
            var_type=var_type,
            length=var_len,
            length_1=len_1,
            length_2=len_2,
            default_value=default_val
        )

        return f'{name} {var_type}{var_len_str} {default_val}', column

    def __create_cobol_pic_item(self, match):
        """
        Create COBOL PIC item from IDMS schema item.

        :param match: Regex match.
        """

        level = match.group('lvl')
        indent = ' ' * 7
        indent += '\t' * int(level)
        name = match.group('name')
        def_val_group_name = 'def_val'
        default_val = '' if match.group(def_val_group_name) is None else f' VALUE {match.group(def_val_group_name)}'
        pic_type_group_name = 'type'
        pic_type = '' if match.group(pic_type_group_name) is None else f' PIC {match.group(pic_type_group_name)}'

        pic = f'{indent}{level} {name}{pic_type}{default_val}.\n'
        self.cobol_out_file.write(pic)

    def __migrate_data(self, file_path: str, mysql_table: MySQLTable):
        """
        Migrate IDMS data file to rows for an existing MySQL table.

        :param file_path: IDMS data file path.
        :param mysql_table: MySQL table object.
        """

        # Get joined columns list
        column_names = mysql_table.get_column_names()
        joined_columns = ',\n'.join(list(map(lambda c: f'\t{c}', column_names)))

        res = f'\nINSERT INTO {mysql_table.name}(\n{joined_columns}\n) VALUES\n'
        rows = list()
        last_primary_key = None

        for line in open(file_path, encoding=self.encoding):
            # Skip "UNLOAD" line
            if line.startswith('UNLOAD '):
                continue

            primary_key = line[:9]

            # Skip if primary key already exists
            if primary_key == last_primary_key:
                log(f'Duplicate primary key "{primary_key}" for table "{mysql_table.name}".', level=logging.WARNING)
                continue

            last_primary_key = primary_key

            # Parse row
            rows.append(mysql_table.parse_idms_row(line))

        # Add rows and closing parentheses into "INSERT" statement
        res += ',\n'.join(rows) + ';\n'

        # Write to output file
        self.mysql_out_file.write(res)

    def __to_mysql_column_name(self, idms_name: str) -> str:
        """
        Format an IDMS PIC name to a MySQL column name.

        :param idms_name: IDMS PIC name.
        :return: MySQL column name.
        """

        return IDMSUtils.name_to_snake_case(idms_name)

    def __migrate_set(self, file_path: str):
        """
        Migrate IDMS set to MySQL foreign key constraints or a view.

        :param file_path: IDMS set file path.
        """

        file = open(file_path)
        file_contents = file.read()

        # Get header from set file to determine whether to migrate to foreign keys or view
        header_match = re.search(IDMS_SET_HEADER_REGEX, file_contents)
        if header_match is None:
            log(f'No header found for IDMS set "{file_path}".', level=logging.ERROR)
            return

        set_name = header_match.group('name')
        mode = header_match.group('mode').lower()

        if mode == 'chain':
            # Migrate chain set to MySQL foreign key constraints
            self.__migrate_chain_set(set_name, file_contents)
        elif mode == 'index':
            # Migrate index set to MySQL view
            self.__migrate_index_set(set_name, file_contents)
        else:
            log(f'Unknown mode "{mode}" encountered in IDMS set "{file_path}".', level=logging.ERROR)
            return

    def __migrate_chain_set(self, set_name: str, file_contents: str):
        """
        Migrate IDMS set of mode "CHAIN" to MySQL foreign key constraints.

        :param set_name: IDMS set name.
        :param file_contents: IDMS set file contents.
        """

        # Skip if shouldn't migrate foreign keys
        if not self.should_migrate_fks:
            log(f'Skipped foreign key creation from IDMS chain set.', level=logging.DEBUG)
            return

        # Get owner
        owner_match = re.search(IDMS_SET_OWNER_REGEX, file_contents)
        if owner_match is None:
            log(f'No owner found for IDMS set with name "{set_name}".', level=logging.ERROR)
            return

        owner_name = owner_match.group('name')
        owner_name = IDMSUtils.name_to_snake_case(owner_name)
        tables_filtered = list(filter(lambda t: t.name == owner_name, self.mysql_tables))

        if len(tables_filtered) == 0:
            log(
                f'Foreign key referencing "{owner_name}" skipped since no matching MySQL table was found.',
                level=logging.WARNING
            )
            return

        # Get members
        for match in re.finditer(IDMS_SET_MEMBER_REGEX, file_contents):
            table_name = match.group('table')
            table_name = IDMSUtils.name_to_snake_case(table_name)
            tables_filtered = list(filter(lambda t: t.name == table_name, self.mysql_tables))

            if len(tables_filtered) == 0:
                log(
                    f'Foreign key referencing "{table_name}" skipped since no matching MySQL table was found.',
                    level=logging.WARNING
                )
                continue

            key = match.group('key')
            key = self.__to_mysql_column_name(key)

            # Form and write SQL statement to output file
            referenced_key = key.replace(key[:4], owner_name[:4].lower(), 1)
            sql = f'\nALTER TABLE {table_name} ADD FOREIGN KEY ({key}) REFERENCES {owner_name}({referenced_key});\n'
            self.mysql_out_file.write(sql)

    def __migrate_index_set(self, set_name: str, file_contents: str):
        """
        Migrate IDMS set of mode "INDEX" to a new MySQL view.

        :param set_name: IDMS set name.
        :param file_contents: IDMS set file contents.
        """

        view_name = IDMSUtils.name_to_snake_case(set_name.replace('IX-', '', 1)) + '_view'
        keys = {}
        from_tables = list()

        member_matches = list(re.finditer(IDMS_SET_MEMBER_REGEX, file_contents))

        # Get members
        for i, mem_match in enumerate(member_matches):
            # Get table name
            table_name = mem_match.group('table')
            table_name = IDMSUtils.name_to_snake_case(table_name)
            from_tables.append(f'\t{table_name}')

            # Get MySQL table
            tables_filtered = list(filter(lambda s: s.name == table_name, self.mysql_tables))

            if len(tables_filtered) == 0:
                log(f'Migrated MySQL table "{table_name}" not found.', level=logging.ERROR)
                continue

            table = tables_filtered[0]

            # Get initial key
            key = mem_match.group('key')
            key = self.__to_mysql_column_name(key)

            if table.has_column(key):
                key = f'\t{table_name}.{key}'
                order = mem_match.group('order')
                keys[key] = f'\t{key} {order}'
            else:
                log(
                    f'Column "{key}" not found in "{table_name}" table while building view from IDMS set "{set_name}".',
                    level=logging.WARNING
                )

            # Get info on next member match to isolate keys
            keys_start = mem_match.end()
            keys_end = member_matches[i + 1].start() - 1 if len(member_matches) > i + 1 else len(file_contents) - 1

            # Get remaining keys
            for key_match in re.finditer(IDMS_SET_MEMBER_KEY_REGEX, file_contents[keys_start:keys_end]):
                key = key_match.group('key')
                key = self.__to_mysql_column_name(key)

                if table.has_column(key):
                    key = f'\t{table_name}.{key}'
                    order = key_match.group('order')
                    keys[key] = f'{key} {order}'
                else:
                    log(
                        f'Column "{key}" not found in "{table_name}" table while building view from IDMS set "{set_name}".',
                        level=logging.WARNING
                    )

        # Generate SQL
        joined_keys = ',\n'.join(keys.keys())
        joined_tables = ',\n'.join(from_tables)
        joined_order = ',\n'.join(keys.values())
        sql = f'\nCREATE VIEW {view_name} AS\nSELECT\n{joined_keys}\nFROM\n{joined_tables}\nORDER BY\n{joined_order};\n'

        # Write to output file
        self.mysql_out_file.write(sql)
