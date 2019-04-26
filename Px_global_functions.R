###########################################################################################
#
# Author: Mohamed Ibrahim (mohamed_ibrahim@mckinsey.com)
# Last modified: 28.3.2019
#
# Script name: 
# Px_global_functions.R
#
# Purpose:
# Compile Functions which are commonly used across the entire pipeline
# 
#
# Input:
# [NA]
#
#
# Output:
# [NA]
# 
#
# Notes:
# [NA]
#
# Issues:
# [No known issues]
#
#
###########################################################################################


#-- Logging into file and print
logit <- function(msg ="", error = 0, warning = 0 , milestone = 0, log_file = "logs.txt")
{
  require(crayon)
  if((error + warning + milestone) < 1)
  {
    cat(blue("[ "+Sys.time()+" ] ")+green(msg+"\n"))
    cat("[ "+Sys.time()+" ] "+msg+"\n",file=log_file,append=TRUE)
    
  }else{
    if(error >0)
    {
      cat(blue("[ "+Sys.time()+" ] ")+red("[ERROR] "+msg+"\n"))
      cat("[ "+Sys.time()+" ] "+"[ERROR] "+msg+"\n",file=log_file,append=TRUE)
      
    }else{
      if(warning>0)
      {
        cat(blue("[ "+Sys.time()+" ] ")+yellow("[WARNING] "+msg+"\n"))
        cat("[ "+Sys.time()+" ] "+"[WARNING] "+msg+"\n",file=log_file,append=TRUE)
        
      }      else{
        if(milestone>0)
        {
          cat(blue("[ "+Sys.time()+" ] ")+cyan(msg+"\n"))
          cat("[ "+Sys.time()+" ] "+msg+"\n",file=log_file,append=TRUE)
          
        }
        
      }
    }
  }
  
}

#-- Dumifying function
get_dummified_dt <- function(dt_in,MAX_STR_LEN=20,N_max_all=5,vars_to_dummyfiy, key_column)
{
  require(data.table)
  require(caret)
  
  i<-1
  
  dt_res = data.table(tmp = dt_in[,get(key_column)])
  setnames(dt_res,"tmp",key_column)
  
  for(curr_var in vars_to_dummyfiy)
  {
    
    dt_mod <-data.table::copy(dt_in)
    
    stats<-dt_in[!is.na(get(curr_var)),.N,by=curr_var][order(-N)]
    if(nrow(stats)>1)
    {
      #-- Optional to see how much we are getting
      stats[,N_cum:=cumsum(N)]
      stats[,perc:=N_cum/sum(stats$N)]
      
      #-- Clean the encoding
      stats[,(curr_var):=iconv(get(curr_var), "latin1", "ASCII", sub="")][,(curr_var):=trimws(tolower(get(curr_var)))]
      stats[,(curr_var):=stri_replace_all(get(curr_var),regex = "[^[:alnum:]]","")][,len:=stri_length(get(curr_var))][len>20,(curr_var):=stri_sub(get(curr_var),1,20)]
      
      dt_mod[,(curr_var):=iconv(get(curr_var), "latin1", "ASCII", sub="")][,(curr_var):=trimws(tolower(get(curr_var)))]
      dt_mod[,(curr_var):=stri_replace_all(get(curr_var),regex = "[^[:alnum:]]","")][,len:=stri_length(get(curr_var))][len>20,(curr_var):=stri_sub(get(curr_var),1,20)]
      
      
      N_max <- min(N_max_all,nrow(stats))
      
      stats[1:N_max,var_:=as.character(get(curr_var))][is.na(var_),var_:="other"]
      
      # cat(paste0("We capture ",round(100*stats[N_max,perc])," % of the data\n"))
      
      na_dt <- data.table(v1=NA_character_,var_="NA")
      setnames(na_dt,"v1",curr_var)
      stats<-rbind(stats,na_dt,fill=T)
      sel_cols <- c(curr_var,"var_")
      stats <- unique(stats[,..sel_cols],by=curr_var)
      frm<- as.formula(paste(curr_var, "var_", sep=" ~ "))
      
      dmy<-dummyVars(frm,data=stats)
      lut<-predict(dmy,stats)
      sel_cols <- c(curr_var)
      lut<-cbind(stats[,..sel_cols],lut)
      cols<-c(key_column,curr_var)
      dumm_cols<-names(lut)[grepl("var_",names(lut))]
      dt_mod<-merge(dt_mod[,..cols],lut,all.x=T,by=curr_var)
      
      #-- Getting the maximum over the whole period
      dt_mod<-dt_mod[, lapply(.SD, max, na.rm=TRUE), by=key_column, .SDcols=dumm_cols] 
      
      #-- Replace infinites
      for (j in 1:ncol(dt_mod)) set(dt_mod, which(is.infinite(dt_mod[[j]])), j, 0)
      
      #-- Include NA in other
      if( "var_NA"%in% names(dt_mod) & "var_other" %in% names(dt_mod))
      {
        dt_mod[,var_other:=var_other+var_NA][,var_NA:=NULL]
        
      }
      if( "var_NA"%in% names(dt_mod) )
      {
        dt_mod[,var_other:=var_NA][,var_NA:=NULL]
        
      }
      #-- Include NA in other
      if( "var_"%in% names(dt_mod) & "var_other" %in% names(dt_mod))
      {
        dt_mod[,var_other:=var_other+var_][,var_:=NULL]
        
      }
      if( "var_"%in% names(dt_mod))
      {
        dt_mod[,var_other:=var_][,var_:=NULL]
        
      }
      
      #-- Renaming
      to_rename<-setdiff(names(dt_mod),key_column)
      setnames(dt_mod,to_rename,unlist(lapply(to_rename,gsub,replacement=curr_var,pattern="var")))
      
      if(i==1)
      {
        
        dt_res<-dt_mod
      }else{
        
        dt_res<-  merge(dt_res,dt_mod,all.x=T,by=key_column)
      }
      
    }
    
    i=i+1
  }
  return(dt_res)
}

#-- Modify the built in + function
"+" = function(x,y) {
  if(is.character(x) || is.character(y)) {
    return(paste(x , y, sep=""))
  } else {
    .Primitive("+")(x,y)
  }
}

#-- Create a DB connection
db_connect<-function(jar_file,usr,pw,hst,prt,db_sid)
{
  require(RJDBC)
  require(data.table)
  require(DBI)
  
  result = tryCatch({
    #-- Create the driver
    drv = JDBC('oracle.jdbc.driver.OracleDriver',classPath = jar_file)
    #-- Create the connection
    conn<-dbConnect(drv,
                    paste0("jdbc:oracle:thin:@(DESCRIPTION= (ADDRESS= (PROTOCOL=TCP) (HOST=",hst,") (PORT=",db_port,")) (CONNECT_DATA= (SID=",db_sid," )))"),  
                    usr,
                    pw
    )
    
  }, warning = function(w) {
    cat(paste0("Warning: ",w,"\n\n"))
  }, error = function(e) {
    cat( red( "[ERROR]: Could not establish a DB connection, here are more details \n"))
    cat(e+"\n\n")
    stop("Exiting")
  }, finally = {
    
  })
  
  return(conn)
}


#-- Function to get the types of the columns of a dataset
get_col_types <- function(dm)
{
  require(data.table)
  
  #-- Get the column types
  dt_vars<-data.table(col_name=names(dm),type=sapply(dm,class))
  
  #-- Label mobile and fixed
  dt_vars[grepl("(mobile|TMH|frame|hwloy|fr_contract|tenure|mob_)",col_name,ignore.case = T),MOBILE:=T][is.na(MOBILE),MOBILE:=F]
  dt_vars[grepl("(fix|OSS|AMD|TOH)",col_name,ignore.case = T),FIX:=T][is.na(FIX),FIX:=F]
  
  #-- Label non predictor variables
  dt_vars[!grepl("(future|vevo_nm|period|vevo_mt|vevo|timestamp|target)",col_name,ignore.case = T),predictors:=T][is.na(predictors),predictors:=F]
  
  #-- We are all historized at this point
  dt_vars[,historized:=T]
  
  return(dt_vars)
}











