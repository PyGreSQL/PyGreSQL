FROM mcr.microsoft.com/devcontainers/base:noble

ENV PYTHONUNBUFFERED=1

# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
# COPY requirements.txt /tmp/pip-tmp/
# RUN pip3 --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
#    && rm -rf /tmp/pip-tmp

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#    && apt-get -y install --no-install-recommends <your-package-list-here>

# Create the Claude Code data directory, so that the claude-data volume
# mounted there is owned by the vscode user and survives container rebuilds
RUN mkdir -p /home/vscode/.claude && chown vscode:vscode /home/vscode/.claude

# Create the GitHub CLI config directory for the gh-config volume,
# so that the gh login survives container rebuilds
RUN mkdir -p /home/vscode/.config/gh \
    && chown -R vscode:vscode /home/vscode/.config

CMD ["sleep", "infinity"]
