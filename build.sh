VERSION_FILE="version.txt"

if [ -f "$VERSION_FILE" ]; then
    VERSION=$(cat "$VERSION_FILE")
else
    echo "Version file not found. Exiting."
    exit 1
fi

NEW_VERSION=$((VERSION + 1))

docker build -f Dockerfile -t assetsense/digitaltwin-datagenerator:v${NEW_VERSION} .

docker push assetsense/digitaltwin-datagenerator:v${NEW_VERSION}

sed -i "s|image: .*|image: assetsense/digitaltwin-datagenerator:v${NEW_VERSION}|" deploy.yaml

echo $NEW_VERSION > $VERSION_FILE

kubectl apply -f deploy.yaml
