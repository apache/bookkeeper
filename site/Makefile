BUNDLER_VERSION = 1.15.1
BUNDLE = bundle _${BUNDLER_VERSION}_
JEKYLL = ${BUNDLE} exec jekyll

clean:
	rm -rf _site

setup:
	gem install bundler \
		-v ${BUNDLER_VERSION} \
		--no-rdoc \
		--no-ri
	NOKOGIRI_USE_SYSTEM_LIBRARIES=true ${BUNDLE} install \
		--path vendor/bundle

build: clean
	${JEKYLL} build \
		--config _config.yml

javadoc:
	scripts/javadoc-gen.sh

serve: build
	${JEKYLL} serve \
		--incremental \
		--config _config.local.yml
