library(RM2)

Fare <- c(150, 100, 50, 250)
Mean <- c(75, 125, 500, 50)
Var <- c(75, 125, 500, 50)
cap <- 400
p <- EMSRb(Fare = Fare, Mean = Mean, Var = Var, cap = cap)

p


set.seed(20)
Fare <- sort(c(150, 100, 50, 250),decreasing = TRUE)
Fare
cap <- 100


T = 3

intens <- function(price) {
  C = 5000
  return(C/price)
}

Mean = Var = intens(Fare)


res = data.frame(timeInMimutes = numeric(cap),
                 price=numeric(cap),residualCapacity=numeric(cap),
                 futureIntensity=numeric(cap)) 



#Start the process of selling the tickets.
#Set the initial price to the price of the lowest class.



t = i = 0
price = min(Fare)
while(t<T & cap>0){
  i = i + 1
  t = t + rexp(1,intens(price))
  p <- EMSRb(Fare = Fare, Mean = (T-t)* intens(Fare), 
             Var = (T-t)*intens(Fare), cap = cap)
  price = max(Fare[p==cap])
  cap = cap - 1
  res[i,] = c(round(t,2),price,cap,round(cap/(T-t),2))
}
print(res)

