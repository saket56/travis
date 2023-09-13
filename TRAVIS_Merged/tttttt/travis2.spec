# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a_travis2 = Analysis(
    ['travis2.py'],
    pathex=[],
    binaries=[],
    datas=[],
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
pyz_travis2 = PYZ(a_travis2.pure, a_travis2.zipped_data, cipher=block_cipher)

exe_travis2 = EXE(
    pyz_travis2,
    a_travis2.scripts,
    [],
    exclude_binaries=True,
    name='travis2',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe_travis2,
    a_travis2.binaries,
    a_travis2.zipfiles,
    a_travis2.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='travis2',
)
