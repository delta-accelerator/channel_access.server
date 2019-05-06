import os
import sys
from setuptools import setup, PEP420PackageFinder, Extension
from setuptools.command.build_ext import build_ext



if 'EPICS_BASE' not in os.environ or 'EPICS_HOST_ARCH' not in os.environ:
    print(sys.stderr, 'EPICS_BASE and EPICS_HOST_ARCH must be set')
    sys.exit(-1)

if sys.platform == 'darwin':
    libsrc = 'Darwin'
    compiler = 'clang'
elif sys.platform.startswith('linux'):
    libsrc = 'Linux'
    compiler = 'gcc'

epics_base = os.environ['EPICS_BASE']
epics_host_arch = os.environ['EPICS_HOST_ARCH']
epics_inc = os.path.join(epics_base, 'include')
epics_lib = os.path.join(epics_base, 'lib', epics_host_arch)


cas_path = 'src/channel_access/server/cas'
cas_extension = Extension('channel_access.server.cas',
    language = 'c++',
    sources = list(map(lambda s: os.path.join(cas_path, s), [
        'cas.cpp',
        'server.cpp',
        'pv.cpp',
        'convert.cpp'
    ])),
    include_dirs = [
        cas_path,
        epics_inc,
        os.path.join(epics_inc, 'os', libsrc),
        os.path.join(epics_inc, 'compiler', compiler),
    ],
    library_dirs = [ epics_lib ],
    runtime_library_dirs = [ epics_lib ],
    extra_compile_args = ['-Wall', '-std=c++11'],
    libraries=['Com', 'ca', 'cas', 'gdd']
)


class BuildExtensionCommand(build_ext):
    user_options = build_ext.user_options + [
        ('with-numpy', None, 'Build with numpy support'),
        ('without-numpy', None, 'Build without numpy support')
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_numpy = False

    def initialize_options(self):
        super().initialize_options()
        self.with_numpy = None
        self.without_numpy = None

    def finalize_options(self):
        super().finalize_options()
        if self.with_numpy is None and self.without_numpy is None:
            try:
                import numpy
            except ImportError:
                self.use_numpy = False
            else:
                self.use_numpy = True
        elif self.without_numpy:
            self.use_numpy = False
        elif self.with_numpy:
            self.use_numpy = True

        if self.define is None:
            self.define = []
        self.define.append(('CA_SERVER_NUMPY_SUPPORT', int(self.use_numpy)))
        if self.use_numpy:
            import numpy
            if self.include_dirs is None:
                self.include_dirs = []
            self.include_dirs.append(numpy.get_include())


with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


setup(
    name = 'channel_access.server',
    description = 'Channel Access server library',
    long_description = long_description,
    license='MIT',
    author = 'André Althaus',
    author_email = 'andre.althaus@tu-dortmund.de',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Scientific/Engineering'
    ],
    keywords = 'epics ca cas channel_access',
    packages = PEP420PackageFinder.find('src'),
    package_dir = { '': 'src' },
    ext_modules = [ cas_extension ],
    python_requires = '>= 3.5',
    setup_requires = [ 'setuptools_scm' ],
    install_requires = [ 'channel_access.common' ],
    extras_require = {
        'numpy': [ 'numpy' ],
        'dev': [ 'tox', 'sphinx', 'pytest' ],
        'doc': [ 'sphinx' ],
        'test': [ 'pytest' ]
    },
    use_scm_version = True,
    cmdclass={
        'build_ext': BuildExtensionCommand,
    }
)
