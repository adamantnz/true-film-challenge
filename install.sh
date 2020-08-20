export KAGGLE_USERNAME=<replace with your username>
export KAGGLE_KEY=<replace with your key>

echo "downloading kaggle dataset..."
mkdir -p data/
cd data/
kaggle datasets download rounakbanik/the-movies-dataset -f movies_metadata.csv
unzip -o *.zip

echo "downloading wikipedia dataset, this will take a few minutes unless (unlike me) you have a really good connection..."
brew install wget
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz