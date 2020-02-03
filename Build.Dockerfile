FROM buildbase
 

RUN mkdir -p /Build/
WORKDIR /Build/
COPY *.spec .
COPY src/ src/
COPY hooks/ hooks/
COPY Makefile .
# COPY version.py src/
ENV PYTHONPATH="src/"
# RUN head -n -1 src/version.py >src/version2.py
# RUN mv src/version2.py src/version.py

# #inject distro into version.py
# RUN echo ",\"distro\":\"\"\"">>src/version.py
# RUN cat /etc/redhat-release >>src/version.py
# RUN echo "\"\"\"">>src/version.py

# #inject libs into version.py
# RUN echo ",\"python_libs\":\"\"\"" >>src/version.py
# RUN pip freeze >>src/version.py
# RUN echo "\"\"\"}" >>src/version.py
#RUN cd src/ && python3 src/setup.py build_ext --inplace

RUN make clean
RUN make exe   

 
  