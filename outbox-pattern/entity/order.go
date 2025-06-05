package entity

type Order struct {
    ID     int
    UserID int
    Amount int64
    Status string
}

type OrderEvent struct {
    OrderID int
    UserID  int
    Amount  int64
    Status  string
}