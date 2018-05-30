
FiletoORC<-function(JDBC_driverClass=NULL,
                         JDBC_classPath=NULL,
                         DB_URL=NULL,
                         DB_NAME=NULL,
                         DB_PW=NULL,
                         DB_TABLE=NULL,
                         DB_RESET=NULL,
                         FilePath=NULL,
                         FileName=NULL,
                         FileSep=NULL,
                         FileDelete=NULL,
                         NumOnePack=NULL) {
  list.of.packages = c("plyr","dplyr","sp","rgdal","RJDBC","rJava",
                       "stringr","data.table","doParallel","foreach","parallel","readr")
  new.packages = list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  lapply(list.of.packages, library, character.only = TRUE)
  memory.limit(5000000)
  options(scipen=999)

  setwd(FilePath)
  cat("Read Data")
  if(grepl(".rds",FileName)){
    DATA_RAW = readRDS(FileName)
  }
  if(grepl(".txt|.csv",FileName)){
    DATA_RAW = read_delim(FileName,delim=FileSep,
                          locale = locale(encoding =
                                            guess_encoding(FileName)[1, 1] %>%
                                            as.character))
  }

  DATA_RAW=data.frame(DATA_RAW)
  divide_section=NumOnePack
  seq=c(rep(1:as.numeric(nrow(DATA_RAW)%/%divide_section),each=divide_section),
        rep(max(as.numeric(nrow(DATA_RAW)%/%divide_section)),each=as.numeric(nrow(DATA_RAW)%%divide_section)))
  DATA_RAW$LYH_SEQ=seq
  cat(paste0("\n SetWordSpace : ",FilePath," \n
             Query Into Oracle Pack : ",NumOnePack ," \n
             DB Table : ",paste0(DB_NAME,".",DB_TABLE)," \n
             Mode : UNION ALL \n
             Copyright : YuHyungLee 2018-05-29"))

  drv <- JDBC(driverClass=JDBC_driverClass,classPath=JDBC_classPath,"'")
  conn <- dbConnect(drv,DB_URL,DB_NAME,DB_PW)
  dbSendUpdate(conn, paste0("TRUNCATE TABLE ",paste0(DB_NAME,".",DB_TABLE)))

  cl <- makeCluster(detectCores())
  clusterExport(cl, ls(), envir=environment())
  clusterEvalQ(cl,{
    library(RJDBC)
    drv <- JDBC(driverClass=JDBC_driverClass,classPath=JDBC_classPath,"'")
    conn <- dbConnect(drv,DB_URL,DB_NAME,DB_PW)
  })
  registerDoParallel(cl)

  start=Sys.time()
  cat(paste("\n Loop Start Time",start))
  start_each = foreach (i=1:max(DATA_RAW$LYH_SEQ), .packages = list.of.packages, .inorder=FALSE,
                        .noexport = "conn") %dopar% {
                          DATA=DATA_RAW %>% filter(LYH_SEQ==i) %>% select(-LYH_SEQ)
                          db_write_table <- function(conn, table, df, if_char_to, if_numeric_to) {

                            # Replace na to null or value
                            df=df %>% mutate_if(is.factor,as.character)
                            character=colnames(df)[sapply(df, class) == "character"]
                            numeric=colnames(df)[sapply(df, class) == "numeric"]
                            df[is.na(character),character] = if_char_to
                            df[is.na(numeric),numeric] = if_numeric_to

                            #Match name, table & df
                            NAME=paste0("SELECT * FROM ",table," WHERE ROWNUM<=1")
                            table_name_list=dbGetQuery(conn,NAME)
                            n=colnames(table_name_list)[colnames(table_name_list) %in% colnames(df)]
                            df=df[,n]

                            # Format data to write
                            batch <- apply(df, 1, FUN = function(x) paste0("'",trimws(x),"'", collapse = ",")) %>%
                              paste0("SELECT ",.," FROM DUAL UNION ALL", collapse = " ")
                            batch=str_sub(batch,1,-11)

                            #Build query
                            query <- paste("INSERT INTO", paste0(table,"(",paste(n,collapse = ","),")"),"\n",
                                           batch)

                            # Send update
                            dbSendUpdate(conn, query)
                          }
                          db_write_table(conn,paste0(DB_NAME,".",DB_TABLE),DATA,"","")
                        }
  clusterEvalQ(cl,{dbDisconnect(conn)})
  stopCluster(cl)
  if(FileDelete==1){
    file.remove(data_name)}
  cat(paste("\n Loop End Time",Sys.time()-start))
}

