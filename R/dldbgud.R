FiletoORC<-function (JDBC_driverClass = NULL, JDBC_classPath = NULL, DB_URL = NULL, 
                     
                     DB_NAME = NULL, DB_PW = NULL, DB_TABLE = NULL, DB_RESET = NULL, 
                     
                     FilePath = NULL, FileName = NULL, FileSep = NULL, FileDelete = NULL, 
                     
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
                 
                 " \n\n             Mode : UNION ALL \n\n             Maker : YuHyungLee 2018-08-20 ver.2",
                 
                 " \n\n             Detail : 1. DB업로드 시 문자 'NA' -> NULL 수정",
                 
                 " \n\n                    : 2. DB업로드 시 숫자 '1000e+1' -> 10000 수정",
                 
                 " \n\n                    : 3. 코어수 입력 추가"))
    } 
  } else{
    DATA_RAW=DataOnR
    rm(DataOnR)
    cat("\n Pass Read Data ")
    cat(paste0("\n SetWorkSpace : ", "On R",
               
               " \n\n             Query Into Oracle Pack : ", NumOnePack,
               
               " \n\n             DB Table : ", paste0(DB_NAME,".",DB_TABLE),
               
               " \n\n             Mode : UNION ALL \n\n             Maker : YuHyungLee 2018-08-20 ver.2",
               
               " \n\n             Detail : 1. DB업로드 시 문자 'NA' -> NULL 수정",
               
               " \n\n                    : 2. DB업로드 시 숫자 지수승 수정",
               
               " \n\n                    : 3. 코어수 입력 추가"))
  }
  
  
  DATA_RAW = data.frame(DATA_RAW)
  
  divide_section = NumOnePack
  
  seq = c(rep(1:as.numeric(nrow(DATA_RAW) %/% divide_section), 
              
              each = divide_section), rep(max(as.numeric(nrow(DATA_RAW) %/% divide_section)), 
                                          
                                          each = as.numeric(nrow(DATA_RAW) %% divide_section)))
  
  DATA_RAW$LYH_SEQ = seq
  

  drv <- JDBC(driverClass = JDBC_driverClass, classPath = JDBC_classPath, "'")
  
  conn <- dbConnect(drv, DB_URL, DB_NAME, DB_PW)
  
  dbSendUpdate(conn, paste0("TRUNCATE TABLE ", paste0(DB_NAME,".", DB_TABLE)))
  
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
    
    conn <- dbConnect(drv, DB_URL, DB_NAME, DB_PW)
    
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
  
  if (FileDelete == 1) {
    
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
                     
