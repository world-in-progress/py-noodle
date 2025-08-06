import shutil
from pathlib import Path
from Cython.Build import cythonize
from setuptools import setup, Extension, find_packages

def create_package_extension(src_dir: Path) -> Extension:
    # Find all .py files recursively
    py_files: list[Path] = []
    for py_file in src_dir.rglob('*.py'):
        if py_file.name != 'setup.py':
            py_files.append(py_file)
            
    # Create package name from directory structure
    package_name = src_dir.name
    
    return Extension(
        name=package_name,
        sources=[str(py_file) for py_file in py_files],
        cython_directives={
            'language_level': '3',
            'package': True
        }
    )
    
if __name__ == '__main__':
    # extensions: list[Extension] = []
    extensions: list[str] = []
    src_dir = Path('./')
    if not src_dir.is_absolute():
        src_dir = Path.cwd() / src_dir

    target_dir = src_dir.parent.parent / src_dir.name
    if not target_dir.exists():
        target_dir.mkdir(parents=True, exist_ok=True)
        
    cython_build_dir = target_dir / 'cython_build'
    if not cython_build_dir.exists():
        cython_build_dir.mkdir(parents=True, exist_ok=True)
        
    build_temp_dir = target_dir / 'build'

    # Compile Cython extensions
    setup(
        ext_modules=cythonize(
            '*.py',
            exclude=['setup.py'],
            language_level='3',
            build_dir=str(cython_build_dir),
            compiler_directives={
                'embedsignature': True,
                'boundscheck': False,
                'wraparound': False,
            },
            nthreads=1,
        ),
        options={
            'build_ext': {
                'build_lib': str(target_dir.parent),
                'build_temp': str(target_dir / 'build'),
            }
        }
    )
    
    # Clean up
    if cython_build_dir.exists():
        shutil.rmtree(cython_build_dir)
    
    if build_temp_dir.exists():
        shutil.rmtree(build_temp_dir)