# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a_home = Analysis(
    ['HomePage.py'],
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
pyz_home = PYZ(a_home.pure, a_home.zipped_data, cipher=block_cipher)

exe_home = EXE(
    pyz_home,
    a_home.scripts,
    [],
    exclude_binaries=True,
    name='HomePage',
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

a_travis1 = Analysis(
    ['friday_launch_oop.py'],
    pathex=[],
    binaries=[('C:\\Users\\abhaykatiwari\\.conda\\envs\\travis\\Library\\bin\\mkl_*.dll', '.')],
    datas=[ ('.\\templates', 'templates'), ('.\\static', 'static'), ('FridayConfig.yaml', '.')],
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
pyz_travis1 = PYZ(a_travis1.pure, a_travis1.zipped_data, cipher=block_cipher)

exe_travis1 = EXE(
    pyz_travis1,
    a_travis1.scripts,
    [],
    exclude_binaries=True,
    name='friday_launch_oop',
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


a_travis2 = Analysis(
    ['travis2.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['babel.numbers','openpyxl.cell._writer'],
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
    exe_home,
    a_home.binaries,
    a_home.zipfiles,
    a_home.datas,
    

    exe_travis1,
    a_travis1.binaries,
    a_travis1.zipfiles,
    a_travis1.datas,
     
    exe_travis2,
    a_travis2.binaries,
    a_travis2.zipfiles,
    a_travis2.datas,

    strip=False,
    upx=True,
    upx_exclude=[],
    name='HomePage',
)

