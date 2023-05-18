from py2exe import freeze

freeze(
    console=['mintAll.py'],
    windows=[],
    data_files=['config.ini'],
    zipfile='library.zip',
    options={},
    version_info={}
)
