## Yield_connection.R

############### CONNECTION CODE ####################


# smart way to check we have specific libs installed or install them otherwise
usePackage <- function(p) {
    if (!is.element(p, installed.packages()[,1])) {
        install.packages(p, dep = TRUE);
    }
    require(p, character.only = TRUE);
}

# send protobuf message with 2 bytes of length before
SendProtobufMsg <- function(con, msg) {
    # 0. check if the message is initialized
    if ( !msg$isInitialized() ) {
        stop(paste("Protobuf message not initialized:", msg$toString()));
    }
    # 1. send message length (2 bytes) as a header
    header <- msg$bytesize();
    writeBin(header, con, size = 2, endian = "big");
    # 2. send message
    msg$serialize(con);
    return;
}


# receive raw protobuf message
ReceiveRawMsg <- function(con) {
    # 1. receive next message length as a 2-byte header
    msg_len <- readBin(con, what = "int", size = 2, signed = FALSE, endian = "big");
    # check received msg_len
    if ( length(msg_len) != 1 ) {
        stop(paste("expected to receive 1 integer as a header while received", length(msg_len)))
    }
    if (msg_len > 100000) { # normally message length shouldn't be > 100000 bytes
        stop(paste("received too large msg length: ", msg_len));
    }
    # 2. receive protobuf-message with specific length
    #return ( readBin(con, what = "raw", n = msg_len) ); ## might return <n bytes!
    msg <- raw(0);
    while (length(msg) < msg_len) {
        chunk <- readBin(con, what = "raw", n = msg_len - length(msg));
        if (length(chunk) > 0) {
            msg <- c(msg, chunk);
        }
        else { # length(chunk) <= 0
            stop("Looks like connection is lost...");
        }
    }
    return (msg);
}


# send login request and receive login reply
Authorize <- function(con, login, pwd, stream_name)
{
    # generate login message
    login_msg <- Authentication.LoginRequest$new(login = login, 
                                                 enc_password = pwd,
                                                 stream_name = stream_name);
    # send login message
    message("Sending login message");
    SendProtobufMsg(con, login_msg);
    
    # receive login-reply message and handle it
    raw_msg <- ReceiveRawMsg(con);
    login_reply <- Authentication.LoginReply$read(raw_msg);
    if (login_reply$connection_status != Authentication.LoginReply$LoginErrorsEnum$OK) {
        stop("Login failed: ", name(Authentication.LoginReply$LoginErrorsEnum$value(number = login_reply$connection_status)) );
    }
    
    # now we're logged in
    message("Logged in successfully as ", login);
}


# main function, connects to server and invokes user specified handler in the event loop
## event handler is expected to have two arguments:
### 1) sold (logical vector of unit length) - TRUE when ticket was sold. Otherwise the event is just a heartbeat.
### 2) price (numeric vector of unit length) - sold price (when sold==TRUE)
## and to return a list of 2 variables:
### 1) lst$change_price (logical vector of unit length) - TRUE when operator desires to change ticket price
### 2) lst$price (numeric vector of unit length) - new ticket price
Connect <- function(host, port, login, pwd, stream_name, event_handler, catch_handler_errors=TRUE, init_price) {
    
    # assert init_price
    stopifnot( is.numeric(init_price), 
               length(init_price) == 1, 
               !is.na(init_price) );
    
    problems_buf_sz <- 10000;
    current_problem_n <- 0;
    result <- list(problems=data.frame(time=.POSIXct(rep(NA, problems_buf_sz)),
                                       problem=character(problems_buf_sz),
                                       stringsAsFactors = FALSE
                                      ),
                   n_signals = 0,
                   pnl = NaN
                   );
    # connect to server
    message("Connecting to ", host, ":", port);
    con <- socketConnection(host, port, blocking = TRUE, server = FALSE, open="r+b", timeout = 120);
    # end of connection handler:
    on.exit( { close(con);  
               message("Connection closed"); 
               message("You sent total of ", result$n_signals, " signal(s) to server");
             } );
    
    # make authorization
    Authorize(con, login, pwd, stream_name);
    
    # send init price from client to server
    Sys.sleep(1);
    message("Sending initial price =", init_price);
    signal_msg <- Yield.Signal$new(price = init_price);
    SendProtobufMsg(con, signal_msg);
    result$n_signals <- result$n_signals + 1;
    message("Initial price sent!");
    
    message("Receiving live datastream");
    
    # event-loop for server messages
    repeat {
        raw_msg <- ReceiveRawMsg(con);
        event_msg <- Yield.Event$read(raw_msg);
        # check error field
        if ( nchar(event_msg$error) > 0 ) {
            problem <- paste("SERVER SENT ERROR: '", event_msg$error, "'", sep='');
            message(problem);
            current_problem_n <- current_problem_n + 1;
            result$problems[current_problem_n,] <- list(Sys.time(), problem);
        }
        # break from repeat-loop in case server stream ends
        if ( event_msg$stream_end ) {
            message("Stream has ended, goodbye!");
            result$pnl = event_msg$pnl;
            message("Your PnL=", result$pnl);
            result$problems <- result$problems[!is.na(result$problems$time),];
            return(result);
        }
        else
        {
            # apply handler to received signal
            lst <- NULL;
            tryCatch({
                lst <- event_handler(event_msg$sold, event_msg$price);
                # assert lst lengths, types and values
                stopifnot( is.logical(lst$change_price), length(lst$change_price) == 1, !is.na(lst$change_price),
                           is.numeric(lst$price), length(lst$price) == 1, 
                           !lst$change_price || !is.na(lst$price) || lst$price > 0
                );
            }, error = function(e) {
                if (catch_handler_errors) {
                    problem <- paste("Error inside handler: ", e, 'No signal sent!', sep='')
                    message('!!!***   WARNING   ***!!!\n', 
                            problem,
                            '\n!!!*******************!!!');
                    current_problem_n <<- current_problem_n + 1;
                    result$problems[current_problem_n,] <<- list(Sys.time(), problem);
                    lst <<- list(change_price=FALSE, price=NaN);
                }
                else {
                    stop(e);
                }
            })
            
            if ( lst$change_price ) {
                message("Changing price to ", lst$price);
                signal_msg <- Yield.Signal$new(price = lst$price);
                # send signal message
                SendProtobufMsg(con, signal_msg);
                result$n_signals <- result$n_signals + 1;
                message("Successfully sent a signal!");
            }
        }
    }
}


# POSIXct time with fractional seconds:
options(digits.secs = 6)
# parse proto-files
usePackage("RProtoBuf");
readProtoFiles(dir="./");    # read all proto-files from current folder
message("Yield_connection.R sourced!");

############# CONNECTION CODE END ##################