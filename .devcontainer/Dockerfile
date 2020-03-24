#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM python:3.6

# Copy default endpoint specific user settings overrides into container to specify Python path
COPY .devcontainer/settings.vscode.json /root/.vscode-remote/data/Machine/settings.json

ENV PYTHONUNBUFFERED 1

# Install git, process tools
RUN apt-get update && apt-get -y install git procps fish postgresql-client default-jre vim lsb-release docker.io
#RUN apt-get -y install python3-tables
RUN mkdir /sonar
WORKDIR /sonar
RUN wget https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.2.0.1873-linux.zip
RUN unzip sonar-scanner-cli-4.2.0.1873-linux.zip \
    && ln -s /sonar/sonar-scanner-4.2.0.1873-linux/bin/sonar-scanner /bin/sonar-scanner
RUN mkdir /workspace


# Install pylint
RUN pip install pylint
RUN pip install pipenv
# Install Python dependencies from requirements.txt if it exists
COPY requirements.txt requirements.txt 
RUN if [ -f "requirements.txt" ]; then pip install -r requirements.txt && rm requirements.txt; fi
# Clean up
# RUN apt-get autoremove -y \
#     && apt-get clean -y \
#     && rm -rf /var/lib/apt/lists/*

#RUN /bin/bash -c "echo alias meta=/workspace/meta_source.sh>>/root/.bashrc"
#RUN /bin/bash -c "echo export LS_OPTIONS='--color=auto'>>/root/.bashrc"
#Default the shell to fish /etc/shells
#RUN chsh -s /usr/bin/fish
#RUN echo -e "task goes here\n$(cat /etc/shells)" > /etc/shells
#vscode settings docker
#/bin/sh -c "/usr/bin/fish && [ -e /bin/bash ] && /bin/bash || /bin/sh"
ENV TZ=America/New_York