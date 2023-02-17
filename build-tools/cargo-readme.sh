set -e
# cargo install cargo-readme
alias readme='cargo readme --no-badges --no-indent-headings --no-license --no-template --no-title'
readme > README.md
cd sea-streamer-types
readme > README.md
echo '' >> ../README.md
readme >> ../README.md
cd ../sea-streamer-socket
readme > README.md
echo '' >> ../README.md
readme >> ../README.md
cd ../sea-streamer-kafka
readme > README.md
echo '' >> ../README.md
readme >> ../README.md
cd ../sea-streamer-stdio
readme > README.md
echo '' >> ../README.md
readme >> ../README.md
cd ..
echo '' >> README.md
cat README_MORE.md >> README.md