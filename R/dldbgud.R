
#' DataFrameToORC
#'
#' This function allows you to upload in oracle DB 
#' @param JDBCClassPath must be inputted. Where is the path to the JAR file containing the driver?
#' @param Url Url must be inppted. What is the DB host, port, sid?
#' @param Id Id must be inppted. What is the DB connecting ID?
#' @param Pw Pw must be inppted. What is the DB connecting password?
#' @param Owner Owner must be inppted. Who created the table?
#' @param Table Table must be inppted. What is the table name?
#' @param Truncate default is FALSE. TRUE is that rows from uploading table delete.
#' @param DataOnR DataOnR must be inppted. What is the data name which uploading in the table?
#' @param NCore defalut is 1. How many use local computer cores? if your all core use, it use detectCores().
#' @param NumOnePack defalut is 500. upload rows number once. if you select NCore 8 and NumOnePack 1000, 8000 rows insert DB once.
#' @keywords dldbgud
#' @examples 
#' DataFrameToORC(
#'  JDBCClassPath = "C:/ojdbc6.jar"
#'  , Url = "jdbc:oracle:thin:@<db_ip>:<db_port>:<db_sid>"
#'  , Id = "LeeYuHyung"
#'  , Pw = "GoodMan"
#'  , Owner = "LYH"
#'  , Table = "TestTable"
#'  , Truncate = FALSE
#'  , DataOnR = "MyData"
#'  , NCore = 1
#'  , NumOnePack = 500
#')

DataFrameToORC = function (
  JDBCClassPath
  , Url
  , Id
  , Pw
  , Owner
  , Table
  , Truncate = FALSE
  , DataOnR
  , NCore = 1
  , NumOnePack = 500
){
  #options
  list.of.packages <- c("plyr", "dplyr","data.table", "stringr","readr",
                        "RJDBC", "rJava", "doParallel", "foreach")
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[, "Package"])]
  if(length(new.packages)) install.packages(new.packages)
  lapply(list.of.packages, library, character.only = TRUE)
  memory.limit(5000000)
  options(scipen = 999)

  DataOnR <- data.frame(DataOnR) %>% mutate_if(is.factor, as.character)
  DataOnR <- replace(DataOnR, is.na(DataOnR), "")
  
  #Assgin Seq by NumOnePack
  DataOnR$Seq <- c(rep(1:as.numeric(nrow(DataOnR) %/% NumOnePack), each = NumOnePack),
                       rep(max(as.numeric(nrow(DataOnR) %/% NumOnePack)), each = as.numeric(nrow(DataOnR) %% NumOnePack)))
  
  #Connecting
  drv <- JDBC(driverClass = "oracle.jdbc.driver.OracleDriver", classPath = JDBCClassPath, "'")
  conn <- dbConnect(drv, URL, Id, Pw)
  
  #Matching Column
  TableName <- dbGetQuery(conn, paste("SELECT * FROM", Owner, ".", Table, "WHERE ROWNUM<=1"))
  DataOnR <- DataOnR[, colnames(TableName)[colnames(TableName) %in% colnames(DataOnR)]]
  
  #Clustering Setting
  cl <- makeCluster(NCore)
  clusterExport(cl, ls(), envir = environment())
  clusterEvalQ(cl, {
    options(scipen = 999)
    library(RJDBC)
    drv <- JDBC(driverClass = "oracle.jdbc.driver.OracleDriver", classPath = JDBCClassPath, "'")
    conn <- dbConnect(drv, URL, Id, Pw)
    }
  )
  registerDoParallel(cl)
  
  #Truncate Table
  if(Truncate) dbSendUpdate(conn, paste("TRUNCATE TABLE", paste(Owner, Table, sep=".")))
  
  #Writing Data func
  db_write_table <- function(conn, SubSetData){
    batch <- apply(SubSetData, 1,
                   FUN = function(x) paste0("'", trimws(x), "'", collapse = ",")) %>% 
      paste("SELECT", ., "FROM DUAL UNION ALL", collapse = " ")
    batch <- str_sub(batch, 1, -11)
    
    query <- paste("INSERT INTO",
                   paste0(paste(Owner, Table, sep="."),
                          "(", paste(colnames(SubSetData), collapse = ","), ")"),
                   "\n", batch)
    dbSendUpdate(conn, query)
  }
    
  #Start Insert
  Start = Sys.time()
  cat("\n Loop Start Times ", Start)
  
  foreach(i = 1:max(DataOnR$Seq),
          .packages = list.of.packages,
          .inorder = FALSE,
          .noexport = "conn") %dopar% {
            SubSet <- DataOnR %>% filter(Seq == i) %>% select(-Seq)
            db_write_table(conn, SubSet)
            }
  
  #Close Connected DB
  clusterEvalQ(cl, {dbDisconnect(conn)})
  stopCluster(cl)
  
  #Check Time
  cat(paste("\n Loop End Times", Sys.time()))
  Etime=as.numeric(trunc(difftime(Sys.time(), Start, units='secs')))
  cat("\n Loop Elapsed Times",paste(Etime %/% 3600, Etime %% 3600 %/% 60, Etime %% 3600 %% 60,sep=":"))
}

FiletoORC<-function (JDBC_driverClass = NULL, JDBC_classPath = NULL, DB_URL = NULL,
                     
                     DB_NAME = NULL,  DB_ID = NULL, DB_PW = NULL, DB_TABLE = NULL, DB_RESET = FALSE, 
                     
                     FilePath = NULL, FileName = NULL, FileSep = NULL, FileDelete = FALSE, 
                     
                     NumOnePack = NULL, NCore = NULL, DataOnR = NULL)
  
{
  
  list.of.packages = c("plyr", "dplyr", "sp", "rgdal", "RJDBC", 
                       
                       "rJava", "stringr", "data.table", "doParallel", "foreach", 
                       
                       "parallel", "readr")
  
  new.packages = list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  
  if (length(new.packages)) install.packages(new.packages)
  
  lapply(list.of.packages, library, character.only = TRUE)
  
  memory.limit(5000000)
  options(scipen = 999)
  
  cat("Please Wait for Reading Data \n")
  
  
  if(is.null(DataOnR)){
    
    setwd(FilePath)
    
    if (grepl(".rds", FileName)) {
      
      DATA_RAW = readRDS(FileName)
      
      cat("\n Read Data Type : '.rds' ")
      
    }
    
    if (grepl(".txt|.csv", FileName)) {
      
      DATA_RAW = read_delim(FileName, delim = FileSep,
                            
                            locale = locale(encoding = guess_encoding(FileName)[1,1] %>% as.character))
      
      cat("\n Read Data Type : '.txt' ")
      
      cat(paste0("\n SetWorkSpace : ", FilePath,
                 
                 " \n\n             Query Into Oracle Pack : ", NumOnePack,
                 
                 " \n\n             DB Table : ", paste0(DB_NAME,".",DB_TABLE),
                 
                 " \n\n             Mode : UNION ALL \n\n             Maker : YuHyungLee 2019-07-01 ver.3",
                 
                 " \n\n             Detail : 1. DB업로드 시 문자 'NA' -> NULL 수정",
                 
                 " \n\n                    : 2. DB업로드 시 숫자 '1000e+1' -> 10000 수정",
                 
                 " \n\n                    : 3. 코어수 입력 추가",
                 
                 " \n\n                    : 4. R상 로딩된 데이터 업로드(기존 파일 따로저장후 다시 불러오는 불편함)",
                 
                 " \n\n                    : 5. DB_ID 추가, 기존 DB_NAME이랑 혼동 (ver.3)"))
    } 
  } else{
    DATA_RAW=DataOnR
    rm(DataOnR)
    cat("\n Pass Read Data ")
      cat(paste0("\n SetWorkSpace : ", FilePath,
                 
                 " \n\n             Query Into Oracle Pack : ", NumOnePack,
                 
                 " \n\n             DB Table : ", paste0(DB_NAME,".",DB_TABLE),
                 
                 " \n\n             Mode : UNION ALL \n\n             Maker : YuHyungLee 2019-07-01 ver.3",
                 
                 " \n\n             Detail : 1. DB업로드 시 문자 'NA' -> NULL 수정",
                 
                 " \n\n                    : 2. DB업로드 시 숫자 '1000e+1' -> 10000 수정",
                 
                 " \n\n                    : 3. 코어수 입력 추가",
                 
                 " \n\n                    : 4. R상 로딩된 데이터 업로드(기존 파일 따로저장후 다시 불러오는 불편함)",
                 
                 " \n\n                    : 5. DB_ID 추가, 기존 DB_NAME이랑 혼동 (ver.3)"))
    } 
  
  
  DATA_RAW = data.frame(DATA_RAW)
  
  divide_section = NumOnePack
  
  seq = c(rep(1:as.numeric(nrow(DATA_RAW) %/% divide_section), 
              
              each = divide_section), rep(max(as.numeric(nrow(DATA_RAW) %/% divide_section)), 
                                          
                                          each = as.numeric(nrow(DATA_RAW) %% divide_section)))
  
  DATA_RAW$LYH_SEQ = seq
  

  drv <- JDBC(driverClass = JDBC_driverClass, classPath = JDBC_classPath, "'")
  
  conn <- dbConnect(drv, DB_URL, DB_ID, DB_PW)
  
  if(DB_RESET){
    dbSendUpdate(conn, paste0("TRUNCATE TABLE ", paste0(DB_NAME,".", DB_TABLE)))
    }
  
  if(is.null(NCore)){
    
    cl <- makeCluster(detectCores())
    
  } else{
    
    cl <- makeCluster(NCore)
    
  }
  
  clusterExport(cl, ls(), envir = environment())
  
  clusterEvalQ(cl, {
    
    library(RJDBC)
    
    drv <- JDBC(driverClass = JDBC_driverClass, classPath = JDBC_classPath, 
                
                "'")
    
    conn <- dbConnect(drv, DB_URL, DB_ID, DB_PW)
    
    options(scipen = 999)
    
  })
  
  registerDoParallel(cl)
  
  start = Sys.time()
  
  cat(paste("\n Loop Start Time ", start))
  
  start_each = foreach(i = 1:max(DATA_RAW$LYH_SEQ), .packages = list.of.packages, .inorder = FALSE, .noexport = "conn") %dopar% {
    
    DATA = DATA_RAW %>% filter(LYH_SEQ == i) %>% select(-LYH_SEQ)
    
    db_write_table <- function(conn, table, df ) {
      
      options(scipen = 999)
      
      df = df %>% mutate_if(is.factor, as.character)
      
      df = replace(df, is.na(df), "")
      
      NAME = paste0("SELECT * FROM ", table, " WHERE ROWNUM<=1")
      
      table_name_list = dbGetQuery(conn, NAME)
      
      n = colnames(table_name_list)[colnames(table_name_list) %in% colnames(df)]
      
      df = df[, n]
      
      batch <- apply(df, 1, FUN = function(x) paste0("'",trimws(x),"'", collapse = ",")) %>%
        
        paste0("SELECT ",.," FROM DUAL UNION ALL", collapse = " ")
      
      batch=str_sub(batch,1,-11)
      
      query <- paste("INSERT INTO", paste0(table, "(",paste(n, collapse = ","), ")"), "\n", batch)
      
      dbSendUpdate(conn, query)
      
    }
    
    db_write_table(conn, paste0(DB_NAME, ".", DB_TABLE),DATA)
    
  }
  
  clusterEvalQ(cl, {
    
    dbDisconnect(conn)
    
  })
  
  stopCluster(cl)
  
  if (FileDelete) {
    
    file.remove(data_name)
    
  }
  
  cat(paste("\n Loop Elapsed Time", Sys.time() - start))
  
}

pause = function(){
  if (interactive())
  {
    invisible(readline(prompt = "Press <Enter> to continue..."))
  }
  else
  {
    cat("Press <Enter> to continue...")
    invisible(readLines(file("stdin"), 1))
  }
}            
                     
