package homework2.model

import java.sql.Timestamp

case class TaxiFact(
                     VendorId: Int,
                     tpep_pickup_datetime: Timestamp,
                     tpep_dropoff_datetime: Timestamp,
                     passenger_count: Int,
                     trip_distance: Double,
                     RatecodeID: Int,
                     store_and_fwd_flag: String,
                     PULocationID: Int,
                     DOLocationID: Int,
                     payment_type: Int,
                     fare_amount: Double,
                     extra: Double,
                     mta_tax: Double,
                     tip_amount: Double,
                     tolls_amount: Double,
                     improvement_surcharge: Double,
                     total_amount: Double
                   )
