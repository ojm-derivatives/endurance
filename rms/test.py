import rms

print("TEST")

rms.rms_main_obj.start_order_and_position_book_fetching()
print(id(rms.rms_main_obj.orders_df))
