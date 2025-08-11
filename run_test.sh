export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
sbt -java-home "$JAVA_HOME" clean test
