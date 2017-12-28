library(ggplot2)
library(ggmap)
library(data.table)
library(maps)

mapWorld <- borders("world", colour="gray50", fill="gray50")
mp <- ggplot() + mapWorld

# ais_df1 = fread("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201704_paint.csv")
# ais_df2 = fread("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201705_paint.csv")
# ais_df3 = fread("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201706_paint.csv")
# ais_df = rbind(ais_df1, ais_df2)
# ais_df = rbind(ais_df, ais_df3)

ais_df = fread("/home/qiu/Documents/sisi2017/xukai_data/4ship_ais/4ship_ais_201708_paint.csv")
ais_df = ais_df[order(ais_df$unique_ID, ais_df$acquisition_time), ]
ais_df$longitude <- as.numeric(ais_df$longitude)
ais_df$latitude <- as.numeric(ais_df$latitude)

ship1_357780000 <- ais_df[ais_df$unique_ID==357780000, ]
ship2_370016000 <- ais_df[ais_df$unique_ID==370016000, ]
ship3_219882000 <- ais_df[ais_df$unique_ID==219882000, ]
ship4_372278000 <- ais_df[ais_df$unique_ID==372278000, ]

ship1_357780000_coor <- c(ship1_357780000$longitude[1], ship1_357780000$latitude[1] + 5)
ship2_370016000_coor <- c(ship2_370016000$longitude[1], ship2_370016000$latitude[1] + 5)
ship3_219882000_coor <- c(ship3_219882000$longitude[1], ship3_219882000$latitude[1] + 5)
ship4_372278000_coor <- c(ship4_372278000$longitude[1], ship4_372278000$latitude[1] + 5)

ship1_357780000_str_coor <- data.frame(longitude=ship1_357780000$longitude[1], 
                                       latitude=ship1_357780000$latitude[1])
ship2_370016000_str_coor <- data.frame(longitude=ship2_370016000$longitude[1], 
                                       latitude=ship2_370016000$latitude[1])
ship3_219882000_str_coor <- data.frame(longitude=ship3_219882000$longitude[1], 
                                       latitude=ship3_219882000$latitude[1])
ship4_372278000_str_coor <- data.frame(longitude=ship4_372278000$longitude[1], 
                                       latitude=ship4_372278000$latitude[1])

ship1_357780000_end_coor <- data.frame(longitude=ship1_357780000$longitude[dim(ship1_357780000)[1]], 
                                       latitude=ship1_357780000$latitude[dim(ship1_357780000)[1]])
ship2_370016000_end_coor <- data.frame(longitude=ship2_370016000$longitude[dim(ship2_370016000)[1]], 
                                       latitude=ship2_370016000$latitude[dim(ship2_370016000)[1]])
ship3_219882000_end_coor <- data.frame(longitude=ship3_219882000$longitude[dim(ship3_219882000)[1]], 
                                       latitude=ship3_219882000$latitude[dim(ship3_219882000)[1]])
ship4_372278000_end_coor <- data.frame(longitude=ship4_372278000$longitude[dim(ship4_372278000)[1]], 
                                       latitude=ship4_372278000$latitude[dim(ship4_372278000)[1]])

mp + 
  geom_path(data=ship1_357780000, aes(x=longitude, y=latitude, group=group), size=1, color="red") + 
  annotate("text", x=ship1_357780000_coor[1], y=ship1_357780000_coor[2], color="red", label=357780000) + 
  geom_path(data=ship2_370016000, aes(x=longitude, y=latitude, group=group), size=1, color="orange") +
  annotate("text", x=ship2_370016000_coor[1], y=ship2_370016000_coor[2], color="orange", label=370016000) + 
  geom_path(data=ship3_219882000, aes(x=longitude, y=latitude, group=group), size=1, color="blue") +
  annotate("text", x=ship3_219882000_coor[1], y=ship3_219882000_coor[2], color="blue", label=219882000) + 
  geom_path(data=ship4_372278000, aes(x=longitude, y=latitude, group=group), size=1, color="green") +
  annotate("text", x=ship4_372278000_coor[1], y=ship4_372278000_coor[2], color="green", label=372278000) + 
  geom_point(data=ship1_357780000_str_coor, aes(x=longitude, y=latitude), color="red", size=5) + 
  geom_point(data=ship2_370016000_str_coor, aes(x=longitude, y=latitude), color="orange", size=5) + 
  geom_point(data=ship3_219882000_str_coor, aes(x=longitude, y=latitude), color="blue", size=5) + 
  geom_point(data=ship4_372278000_str_coor, aes(x=longitude, y=latitude), color="green", size=5) +
  geom_point(data=ship1_357780000_end_coor, aes(x=longitude, y=latitude), color="red4", size=5) + 
  geom_point(data=ship2_370016000_end_coor, aes(x=longitude, y=latitude), color="orange4", size=5) + 
  geom_point(data=ship3_219882000_end_coor, aes(x=longitude, y=latitude), color="blue4", size=5) + 
  geom_point(data=ship4_372278000_end_coor, aes(x=longitude, y=latitude), color="green4", size=5) + 
  ggtitle("201708 Trajectory - 淡色为起点 深色为终点")
