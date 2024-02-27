override PKG=esr_weather


build_pkg:
	rm -rf $(PKG).egg*
	rm -rf dist
	python setup.py sdist bdist_wheel
	pip install .

install_twine:
	pip install twine

upload_pkg: 
	twine upload dist/*

publish: build_pkg upload_pkg

install_bufr:
	rm -rf NCEPLIBS-bufr
	git clone --depth 1 https://github.com/NOAA-EMC/NCEPLIBS-bufr.git
	cd NCEPLIBS-bufr && mkdir build && cmake -DCMAKE_INSTALL_PREFIX=$PWD && make -j4 && make install
	cd ..
	rm -rf rda-bufr-decode-ADPsfc
	git clone --depth 1 https://github.com/NCAR/rda-bufr-decode-ADPsfc.git
	sed -i 's|LIB=/path/to/bufrlib.a|LIB=../../NCEPLIBS-bufr/src/libbufr_4.a|' rda-bufr-decode-ADPsfc/install/install.sh
	cd rda-bufr-decode-ADPsfc/install && ./install.sh
