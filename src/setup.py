from distutils.core import setup, Extension

nodecache = Extension('nodecache',
                    sources=['pbfdump/nodecache.cpp'],
                    include_dirs=[ '/usr/local/include', '/usr/include'],
                    libraries=['boost_python'],
                    library_dirs=['/usr/local/lib', '/usr/lib'],

                    )

setup (name='nodecache',
       version='1.0',
       description='This is a demo package',
       ext_modules=[nodecache])
