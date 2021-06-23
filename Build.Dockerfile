FROM buildbase
 
USER root
RUN mkdir -p /Build/
WORKDIR /Build/
 
 

#RUN yum install -y postgresql-devel python3-devel
 
RUN pip3 install wheel
RUN pip3 install --upgrade pip 


COPY *.spec .
COPY src/ src/
COPY hooks/ hooks/
COPY Makefile .
# COPY version.py src/

ENV PYTHONPATH="src/:/usr/local/lib64/python3.6/site-packages/"



COPY requirements.txt src/
#RUN cat src/requirements.txt
RUN pip3 install -r src/requirements.txt
RUN ls -la src/
RUN ls -la .
RUN pwd
#RUN pip3 install pandas twine pyyaml pytest-cov xlrd
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
RUN sed -i "/:=/d" Makefile
#RUN cat Makefile
RUN make clean
RUN make exe   
 
CMD ["/bin/sh"]
 
  