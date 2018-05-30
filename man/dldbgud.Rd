\name{FiletoORC}
\title{Fast upload to Oracle DB}
\arguments{
\item{JDBC_driverClass}{name of the Java class of the JDBC driver to load. If empty, it is assumed that corresponding JDBC drivers were loaded by other means.}
\item{JDBC_classPath}{class path that needs to be appended in order to load the desired JDBC driver. Usually it is the path to the JAR file containing the driver.}
\item{DB_NAME,DB_PW,DB_TABLE}{authentication arguments needed by the DBMS instance. For details see the appropriate DBIDriver.}
\item{DB_RESET}{if 1 ,TRUNCATE TABLE}
\item{FileSep}{if rds, no matter seperate}
\item{FileDelete}{if 1, delete file}
\item{NumOnePack}{once sendquery pack}
}
\description{
file(.txt,.rds,.csv) to Oracle DB Using your computer cpu.
}
\examples{
FiletoDatabase(
JDBC_driverClass="oracle.jdbc.driver.OracleDriver",
               JDBC_classPath="dir/ojdbc6.jar",
               DB_URL="jdbc:oracle:thin:@x.x.x.x:1521:ORCL",
               DB_NAME="db_name",
               DB_PW="db_password",
               DB_TABLE="SAMPLE_DB",
               DB_RESET=1,
               FilePath="dir",
               FileName="sample.txt",
               FileSep="|",
               FileDelete=0,
               NumOnePack=700)
}
