library(testthat)

test_that("Extra functions work on regular DBI connection", {
  connection <- DBI::dbConnect(
    drv = duckdb::duckdb(),
    dbdir = ":memory:"
  )
  on.exit(DBI::dbDisconnect(connection))
  insertTable(
    connection = connection,
    tableName = "cars",
    data = cars
  )

  cars2 <- querySql(connection, "SELECT * FROM cars;")  
  expect_equal(cars, cars2)
  
  # hash <- computeDataHash(connection, "main")
})