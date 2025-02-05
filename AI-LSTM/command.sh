docker build -t tf1_jupyter

docker run -p 8888:8888 -v $(pwd):/tf1_notebook tf1_jupyter