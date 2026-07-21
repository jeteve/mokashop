#! /usr/bin/bash

set -xe
whoami
curl -OJL https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_amd64.deb
sudo dpkg -i grpcurl_1.9.3_linux_amd64.deb
rm -f grpcurl_1.9.3_linux_amd64.deb

