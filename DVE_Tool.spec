# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['C:/Users/dsingh35/Downloads/newvolvo/Volvo/DVE_Tool.py'],
    pathex=[],
    binaries=[],
    datas=[('C:/Users/dsingh35/Downloads/Volvo/static', 'static/'), ('C:/Users/dsingh35/Downloads/Volvo/templates', 'templates/'), ('C:/Users/dsingh35/Downloads/newvolvo/Volvo/datacompy', 'datacompy/')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='DVE_Tool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='C:\\Users\\dsingh35\\Downloads\\construct.ico',
)
