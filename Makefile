BUILDUI=./utils/buildui.py -5

UIC_SOURCE_FILES=$(wildcard data/ui/trayicon/*.ui)
UIC_PYTHON_FILES=$(patsubst data/ui/%.ui,qtborg/%.py,$(UIC_SOURCE_FILES))

TS_FILES=$(wildcard translations/*.ts)

all: $(UIC_PYTHON_FILES)

clean:
	rm -rf $(UIC_PYTHON_FILES)

lupdate:
	pylupdate5 -verbose mlxc-qt.pro

lrelease: $(TS_FILES)
	lrelease-qt5 mlxc-qt.pro

$(UIC_PYTHON_FILES): qtborg/%.py: data/ui/%.ui
	$(BUILDUI) $< $@


.PHONY: lupdate
