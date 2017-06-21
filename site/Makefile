BUNDLER_VERSION = 1.15.1
BUNDLE = bundle _${BUNDLER_VERSION}_
JEKYLL = ${BUNDLE} exec jekyll

setup:
	gem install bundler \
		-v ${BUNDLER_VERSION} \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true ${BUNDLE} install \
		--path vendor/bundle

build:
	${JEKYLL} build

javadoc:
	scripts/javadoc-gen.sh

serve:
	${JEKYLL} serve \
		--incremental
