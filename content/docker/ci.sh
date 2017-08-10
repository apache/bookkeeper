
set -e -x -u

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

export IMAGE_NAME="bookkeeper/docs"

pushd ${SCRIPT_DIR}

docker build --rm=true -t ${IMAGE_NAME} .

popd

if [ "$(uname -s)" == "Linux" ]; then
  USER_NAME=${SUDO_USER:=$USER}
  USER_ID=$(id -u "${USER_NAME}")
  GROUP_ID=$(id -g "${USER_NAME}")
  LOCAL_HOME="/home/${USER_NAME}"
else # boot2docker uid and gid
  USER_NAME=$USER
  USER_ID=1000
  GROUP_ID=50
  LOCAL_HOME="/Users/${USER_NAME}"
fi

docker build -t "${IMAGE_NAME}-${USER_NAME}" - <<UserSpecificDocker
FROM ${IMAGE_NAME}
RUN groupadd --non-unique -g ${GROUP_ID} ${USER_NAME} && \
  useradd -g ${GROUP_ID} -u ${USER_ID} -k /root -m ${USER_NAME}
ENV  HOME /home/${USER_NAME}
UserSpecificDocker

BOOKKEEPER_DOC_ROOT=${SCRIPT_DIR}/..

pushd ${BOOKKEEPER_DOC_ROOT}

docker run \
  --rm=true \
  -w ${BOOKKEEPER_DOC_ROOT} \
  -u "${USER}" \
  -v "${BOOKKEEPER_DOC_ROOT}:${BOOKKEEPER_DOC_ROOT}" \
  -v "${LOCAL_HOME}:/home/${USER_NAME}" \
  -p 4000:4000 \
  -e JEKYLL_ENV=production \
  ${IMAGE_NAME}-${USER_NAME} \
  bash -c "make setup && make apache"

popd

