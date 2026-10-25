FROM apache/spark:4.1.3-python3

USER root

# El requirements.txt vive en la raíz del contexto de build (etapa2/)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV SPARK_DRIVER_HOST=jupyter

WORKDIR /home/notebooks

EXPOSE 8888 4040

CMD ["jupyter", "lab", \
     "--allow-root", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--NotebookApp.token=''", \
     "--NotebookApp.password=''", \
     "--notebook-dir=/home/notebooks"]
