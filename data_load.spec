# -*- mode: python ; coding: utf-8 -*-

block_cipher = None


a = Analysis(['src/py_dbmigration/data_load.py'],
             pathex=['/workspace'],
             binaries=[],
             datas=[('src/py_dbmigration/data_file_mgnt/logic_sql.yml', 'py_dbmigration/data_file_mgnt/')],
             hiddenimports=[
                'py_dbmigration.custom_logic.abort_if_obsolete'
                ,'py_dbmigration.custom_logic.pg_bulk_copy'
                ,'py_dbmigration.custom_logic.extract_compressed_file'
                ,'py_dbmigration.custom_logic.update_child_file'                
                ,'py_dbmigration.custom_logic.read_from_s3'
                ,'py_dbmigration.custom_logic.append_file_id_w_header'
                ,'py_dbmigration.custom_logic.append_file_id'
                ,'py_dbmigration.custom_logic.load_status'
                ,'py_dbmigration.custom_logic.count_file_linux_wc'
                ,'py_dbmigration.custom_logic.sasreader'
                ,'py_dbmigration.custom_logic.sed_modify_file'
                ,'py_dbmigration.custom_logic.generate_checksum'
                ,'py_dbmigration.custom_logic.pandas_import'
                ,'py_dbmigration.custom_logic.load_status_post_import'
                ,'py_dbmigration.custom_logic.row_count_excel'
                ,'py_dbmigration.custom_logic.pandas_import_excel'
                ,'py_dbmigration.custom_logic.pandas_import_xml'
                ,'py_dbmigration.custom_logic.purge_temp_file'
                ,'py_dbmigration.custom_logic.log_file_size'
                ,'py_dbmigration.custom_logic.extract_msaccess_csv'
                ,'py_dbmigration.custom_logic.abort_if_duplicate'
                ,'py_dbmigration.custom_logic.upsert_table_from_stg'
                ,'py_dbmigration.custom_logic.copy_file'
                ,'py_dbmigration.utils.func'
                ,'xlrd'
             ],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='data_load',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=False )
