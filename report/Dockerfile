ARG ALPINE_VERSION=3.16

# 2-pass thing for building mmdc, via https://github.com/nodejs/docker-node/blob/main/docs/BestPractices.md#smaller-images-without-npmyarn
FROM node:18-alpine${ALPINE_VERSION} AS mermaid-builder
WORKDIR /build-stage
RUN npm init -y && npm install @mermaid-js/mermaid-cli@11.12.0

# slightly janky debloating
RUN <<EOF
# we don't need any source maps (takes us from 290MB to 200MB)
find node_modules -type f -name '*.map' -delete
EOF


FROM alpine:${ALPINE_VERSION}

# copy mermaid stuff in from its builder image
COPY --from=mermaid-builder /usr/local/bin/node /usr/local/bin/
COPY --from=mermaid-builder /build-stage/node_modules /mermaid/node_modules
# deps for node
RUN apk add --no-cache libstdc++ dumb-init
# deps for mermaid (via their mmdc dockerfile)
ENV CHROME_BIN="/usr/bin/chromium-browser" PUPPETEER_SKIP_DOWNLOAD="true"
RUN apk add chromium font-noto-cjk font-noto-emoji terminus-font ttf-dejavu ttf-freefont ttf-font-awesome ttf-inconsolata ttf-linux-libertine && fc-cache -f
COPY <<EOF /mermaid/puppeteer-config.json
{
    "executablePath": "/usr/bin/chromium-browser",
    "args": [
        "--no-sandbox"
    ]
}
EOF

# quick bash script so we don't need to manually pass in this config file for normal usage
COPY <<EOF /usr/local/bin/mmdc
#!/usr/bin/env sh
/mermaid/node_modules/.bin/mmdc -p /mermaid/puppeteer-config.json "\$@"
EOF
RUN chmod +x /usr/local/bin/mmdc

################################################################################
# setup latex stuff.
# (this is all yoinked pretty much 1-to-1 from kjarosh/latex-docker,
#  but i needed a different base alpine version so here we are.)
################################################################################
ARG TL_MIRROR="https://texlive.info/tlnet-archive/2025/05/01/tlnet/"
RUN apk add --no-cache perl curl fontconfig libgcc gnupg && \
    mkdir "/tmp/texlive" && cd "/tmp/texlive" && \
    wget "$TL_MIRROR/install-tl-unx.tar.gz" && \
    tar xzvf ./install-tl-unx.tar.gz && \
    ( \
        echo "selected_scheme scheme-minimal" && \
        echo "instopt_adjustpath 0" && \
        echo "tlpdbopt_install_docfiles 0" && \
        echo "tlpdbopt_install_srcfiles 0" && \
        echo "TEXDIR /opt/texlive/" && \
        echo "TEXMFLOCAL /opt/texlive/texmf-local" && \
        echo "TEXMFSYSCONFIG /opt/texlive/texmf-config" && \
        echo "TEXMFSYSVAR /opt/texlive/texmf-var" && \
        echo "TEXMFHOME ~/.texmf" \
    ) > "/tmp/texlive.profile" && \
    "./install-tl-"*"/install-tl" --location "$TL_MIRROR" -profile "/tmp/texlive.profile" && \
    rm -vf "/opt/texlive/install-tl" && \
    rm -vf "/opt/texlive/install-tl.log" && \
    rm -vrf /tmp/*
ENV PATH="${PATH}:/opt/texlive/bin/x86_64-linuxmusl"
################################################################################

# basic latex deps
RUN tlmgr install scheme-minimal latex-bin latex-tools
# our latex deps
RUN tlmgr install babel ieeetran hyperref etoolbox l3packages lipsum epstopdf-pkg

RUN apk add make
