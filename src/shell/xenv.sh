alias egrep='grep -i -E'
alias qfind='sudo updatedb; locate'

export PS1="\[\e[37;40m\][\[\e[34;40m\]\u\[\e[37;40m\]@\[\e[35;40m\]\h \[\e[36;40m\]\W\[\e[0m\]]\\$ "

# JAVA_HOME
export JAVA_HOME=/opt/module/jdk1.8.0_212
export PATH=$PATH:$JAVA_HOME/bin

# HADOOP_HOME
export HADOOP_HOME=/opt/module/hadoop-3.1.3
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin

# KAFKA_HOME
export KAFKA_HOME=/opt/module/kafka_2.11-0.11.0.0
export PATH=$PATH:$KAFKA_HOME/bin

# FLUME_HOME
export FLUME_HOME=/opt/module/tmp5/apache-flume-1.9.0
export PATH=$PATH:$FLUME_HOME/bin
