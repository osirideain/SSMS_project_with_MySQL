import xml.etree.ElementTree as ET
import pyodbc
import ftplib
import pymysql
from iteration_utilities import duplicates

def process(arg):
    global user_xml
    user_xml = nested_list[x][0]
    global table
    table = nested_list[x][1]
    global other_table
    other_table= nested_list[x][2]
    global fileName_new
    fileName_new = nested_list[x][3]
    if nested_list[x] == nested_list[x][0]:
         return nested_list[x][0]
    if nested_list[x] == nested_list[x][1]:
         return nested_list[x][1]
    if nested_list[x] == nested_list[x][2]:
         return nested_list[x][2]
    if nested_list[x] == nested_list[x][3]:
         return nested_list[x][3] 

    def CreateTable():
        conn = pymysql.connect(host='' ,
                                 port=10065,
                                 user='',
                                 password='',
                                 db='')
        mytree=ET.parse(user_xml)
        root=mytree.getroot()        
     
        other_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag is not None:
                other_list.append(x.tag)
                for y in x:
                    if y.tag is not None:
                        other_list.append(y.tag)
        duplicate_list = list(set(duplicates(other_list)))
        tag_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag not in duplicate_list:
                tag_list.append(x.tag)
            for y in x:
                if y.tag not in duplicate_list and y.tag is not None:
                    tag_list.append(y.tag)
        for x in tag_list:
            if x == "DELIVERY":
                tag_list.remove(x)
            if x == "CATEGORIES":
                tag_list.remove(x)
            if "IMGURL" not in tag_list:
                tag_list.insert(6, "IMGURL")

        tag_list = " text, ".join(tag_list)
##        create_new_table_1 = """
##        create table %s(
##        %s
##        )
##        """%(table,tag_list)
        with conn.cursor() as cursor:
            cursor.execute("create table {}({})".format(table, tag_list+ " text"))
            conn.commit()
        print("{} Completed".format(table))

        def tag():
            def param():
                create_new_table = """
                create table %s(product_nums text, param_names text, vals text
                )
                """%(other_table)
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()
                    print("{} Completed".format(other_table))


            def altimg():
                create_new_table = """
                create table %s( item_ids text,images text, alternative_images text
                )
                """%(other_table)
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()
                    print("{} Completed".format(other_table))
            for x in root.find("./SHOPITEM"):
                if x.tag == "PARAM":
                    return param()
                elif x.tag == "IMGURL_ALTERNATIVE":
                    return altimg()

        tag()
        
        def other_tag():
            def param1():
                create_new_table = """
                create table %s(images text, other_images text
                )
                """%(other_table+"_1")
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()
                print("{} Completed".format(other_table+"_1"))

            for x in root.find("./SHOPITEM"):
                if x.tag == "ADDIMG":
                    return param1()
        other_tag()
    CreateTable()
    
    def CreateXML():
        conn = pymysql.connect(host='' ,
                                 port=10065,
                                 user='',
                                 password='',
                                 db='')

        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        other_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag is not None:
                other_list.append(x.tag)
                for y in x:
                    if y.tag is not None:
                        other_list.append(y.tag)
        duplicate_list = list(set(duplicates(other_list)))
        tag_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag not in duplicate_list:
                tag_list.append(x.tag)
            for y in x:
                if y.tag not in duplicate_list and y.tag is not None:
                    tag_list.append(y.tag)
        for x in tag_list:
            if x == "DELIVERY":
                tag_list.remove(x)
            if x == "CATEGORIES":
                tag_list.remove(x)
            if "IMGURL" not in tag_list:
                tag_list.insert(6, "IMGURL")
        
        values = []
        for x in range(len(tag_list)):
            x = "%s"
            values.append(x)
        values = ", ".join(values)
        items_list = []
        for x in root.findall('./SHOPITEM'):
            def shopitems():
                list = []
                def itemIDS():
                    def itemID():
                        itemID = x.find('ITEM_ID')
                        if itemID is not None:
                            return itemID.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "ITEM_ID":
                            return itemID()

                for b in range(len(tag_list)):
                    if tag_list[b] == "ITEM_ID":
                        list.insert(b, itemIDS())

                def productNums():
                    def productNum():
                        productNum = x.find('PRODUCTNO')
                        if productNum is not None:
                            return productNum.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PRODUCTNO":
                            return productNum()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PRODUCTNO":
                        list.insert(b, productNums())

                def productNames():
                    def productName():
                        productName = x.find('PRODUCTNAME')
                        if productName is not None:
                            return productName.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PRODUCTNAME":
                            return productName()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PRODUCTNAME":
                        list.insert(b, productNames())


                def products():
                    def product():
                        product = x.find('PRODUCT')                
                        if product is not None:
                            return product.text
                        else:
                            return "0"
                    for a in root.find("SHOPITEM"):
                        if a.tag == "PRODUCT":
                            return product()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PRODUCT":
                        list.insert(b, products())


                def descriptions():
                    def description():
                        description = x.find('DESCRIPTION')
                        if description is not None:
                            return description.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "DESCRIPTION":
                            return description()

                for b in range(len(tag_list)):
                    if tag_list[b] == "DESCRIPTION":
                        list.insert(b, descriptions())

                        
                def urls():
                    def url():
                        url = x.find('URL')
                        if url is not None:
                            return url.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "URL":
                            return url()

                for b in range(len(tag_list)):
                    if tag_list[b] == "URL":
                        list.insert(b, urls())


                def images():
                    def image():
                        image = x.find('IMGURL')
                        if image is not None:
                            return image.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "IMGURL":
                            return image()

                for b in range(len(tag_list)):
                    if tag_list[b] == "IMGURL":
                        list.insert(b, images())


                def prices():
                    def price():
                        price = x.find('PRICE')
                        if price is not None:
                            return price.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PRICE":
                            return price()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PRICE":
                        list.insert(b, prices())


                def video_urls():
                    def video_url():
                        video_url = x.find('VIDEO_URL')
                        if video_url is not None:
                            return video_url.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "VIDEO_URL":
                            return video_url()

                for b in range(len(tag_list)):
                    if tag_list[b] == "VIDEO_URL":
                        list.insert(b, video_urls())


                def price_vats():
                    def price_vat():
                        price_vat = x.find('PRICE_VAT')
                        if price_vat is not None:
                            return price_vat.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PRICE_VAT":
                            return price_vat()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PRICE_VAT":
                        list.insert(b, price_vats())


                def vats():
                    def vat():
                        vat = x.find('VAT')
                        if vat is not None:
                            return vat.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "VAT":
                            return vat()

                for b in range(len(tag_list)):
                    if tag_list[b] == "VAT":
                        list.insert(b, vats())


                def manufacturers():
                    def manufacturer():
                        manufacturer = x.find('MANUFACTURER')
                        if manufacturer is not None:
                            return manufacturer.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "MANUFACTURER":
                            return manufacturer()

                for b in range(len(tag_list)):
                    if tag_list[b] == "MANUFACTURER":
                        list.insert(b, manufacturers())


                def categories():
                    def category():
                        category = x.find('CATEGORYTEXT')
                        if category is not None:
                            return category.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "CATEGORYTEXT":
                            return category()

                for b in range(len(tag_list)):
                    if tag_list[b] == "CATEGORYTEXT":
                        list.insert(b, categories())


                def categories_1():
                    def category_1():
                        for category in root.findall(".//CATEGORIES"):
                            categories = category.find('CATEGORY_NAME')
                            if categories is not None:
                                return categories.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "CATEGORIES":
                            return category_1()
                        
                for b in range(len(tag_list)):
                    if tag_list[b] == "CATEGORY_NAME":
                        list.insert(b, categories_1())


                def categories_levels():
                    def category_level():
                        for category in root.findall(".//CATEGORIES"):
                            categories = category.find('CATEGORY_LEVEL')
                            if categories is not None:
                                return categories.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "CATEGORIES":
                            return category_level()

                for b in range(len(tag_list)):
                    if tag_list[b] == "CATEGORY_LEVEL":
                        list.insert(b, categories_levels())


                def heureka_cpcs():
                    def heureka_cpc():
                        heureka_cpc = x.find('HEUREKA_CPC')
                        if heureka_cpc is not None:
                            return heureka_cpc.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "HEUREKA_CPC":
                            return heureka_cpc()

                for b in range(len(tag_list)):
                    if tag_list[b] == "HEUREKA_CPC":
                        list.insert(b, heureka_cpcs())


                def delivery_dates():
                    def delivery_date():
                        delivery_date = x.find('DELIVERY_DATE')
                        if delivery_date is not None:
                            return delivery_date.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "DELIVERY_DATE":
                            return delivery_date()

                for b in range(len(tag_list)):
                    if tag_list[b] == "DELIVERY_DATE":
                        list.insert(b, delivery_dates())


                def eans():
                    def ean():
                        ean = x.find('EAN')
                        if ean is not None:
                            return ean.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "EAN":
                            return ean()

                for b in range(len(tag_list)):
                    if tag_list[b] == "EAN":
                        list.insert(b, eans())


                def delivery_ids():
                    def delivery_id():
                        for info in root.findall('.//DELIVERY'):
                            id_info = info.find('DELIVERY_ID')
                            if id_info is not None:
                                return id_info.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "DELIVERY":
                            return delivery_id()

                for b in range(len(tag_list)):
                    if tag_list[b] == "DELIVERY_ID":
                        list.insert(b, delivery_ids())

                            
                def delivery_prices():
                    def delivery_price():
                        for info in root.findall('.//DELIVERY'):
                            price = info.find('DELIVERY_PRICE')
                            if price is not None:
                                return price.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "DELIVERY":
                            return delivery_price()

                for b in range(len(tag_list)):
                    if tag_list[b] == "DELIVERY_PRICE":
                        list.insert(b, delivery_prices())

                            
                def delivery_price_codes():
                    def delivery_price_code():
                        for info in root.findall('.//DELIVERY'):
                            price_code = info.find('DELIVERY_PRICE_COD')
                            if price_code is not None:
                                return price_code.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "DELIVERY":
                            return delivery_price_code()

                for b in range(len(tag_list)):
                    if tag_list[b] == "DELIVERY_PRICE_COD":
                        list.insert(b, delivery_price_codes())


                def param_names():
                    def param_name():
                        for param_name in root.findall(".//PARAM"):
                            param_names = param_name.find('PARAM_NAME')
                            if param_names is not None:
                                return param_names.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PARAM":
                            return param_name()

                for b in range(len(tag_list)):
                    if tag_list[b] == "PARAM_NAME":
                        list.insert(b, param_names())

                    
##                def vals():
##                    def val():
##                        val = x.find('VAL')
##                        if val is not None:
##                            return val.text
##                        else:
##                            return "0"
##                    for a in root.find("./SHOPITEM"):
##                        if a.tag == "VAL":
##                            return val()
##
##                for b in range(len(tag_list)):
##                    if tag_list[b] == "VAL":
##                        list.insert(b, vals())


                def vals_1():
                    def val_1():
                        for val in root.findall(".//PARAM"):
                            vals = val.find('VAL')
                            if vals is not None:
                                return vals.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "PARAM":
                            return vals()

                for b in range(len(tag_list)):
                    if tag_list[b] == "VAL":
                        list.insert(b, vals_1())


                def item_groups():
                    def item_group():
                        item_group = x.find('ITEMGROUP_ID')
                        if item_group is not None:
                            return item_group.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "ITEMGROUP_ID":
                            return item_group()

                for b in range(len(tag_list)):
                    if tag_list[b] == "ITEMGROUP_ID":
                        list.insert(b, item_groups())

                def avails():
                    def avail():
                        avail = x.find('AVAILABILITY')
                        if avail is not None:
                            return avail.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "AVAILABILITY":
                            return avail()

                for b in range(len(tag_list)):
                    if tag_list[b] == "AVAILABILITY":
                        list.insert(b, avails())


                def sales():
                    def sale():
                        sale = x.find('SALE')
                        if sale is not None:
                            return sale.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "SALE":
                            return sale()

                for b in range(len(tag_list)):
                    if tag_list[b] == "SALE":
                        list.insert(b, sales())


                def accesses():
                    def access():
                        access = x.find('ACCESSORY')
                        if access is not None:
                            return access.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "ACCESSORY":
                            return access()

                for b in range(len(tag_list)):
                    if tag_list[b] == "ACCESSORY":
                        list.insert(b, accesses())


##                def extendwar_vals():
##                    def extendwar_val():
##                        for ext in root.findall(".//EXTENDED_WARRANTY"):
##                            exts = ext.find('VAL')
##                            if exts is not None:
##                                return exts.text
##                            else:
##                                return "0"
##                    for a in root.find("./SHOPITEM"):
##                        if a.tag == "EXTENDED_WARRANTY":
##                            return extendwar_val()
##
##                for b in range(len(tag_list)):
##                    if tag_list[b] == "VAL":
##                        list.insert(b, extendwar_vals())
##
##                    
##                def extendwar_descs():
##                    def extendwar_desc():
##                        for ext in root.findall(".//EXTENDED_WARRANTY"):
##                            exts = ext.find('DESC')
##                            if exts is not None:
##                                return exts.text
##                            else:
##                                return "0"
##                    for a in root.find("./SHOPITEM"):
##                        if a.tag == "EXTENDED_WARRANTY":
##                            return extendwar_desc()
##
##                for b in range(len(tag_list)):
##                    if tag_list[b] == "DESC":
##                        list.insert(b, extendwar_descs())

            
                def special_servs():
                    def special_serv():
                        special_serv = x.findall('SPECIAL_SERVICE')
                        if special_serv is not None:
                            return special_serv.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "SPECIAL_SERVICE":
                            return special_serv()

                for b in range(len(tag_list)):
                    if tag_list[b] == "SPECIAL_SERVICE":
                        list.insert(b, special_servs())

            
                def salesvouch_cods():
                    def salesvouch_cod():
                        for sale in root.findall(".//SALES_VOUCHER"):
                            cod = sale.find('CODE')
                            if cod is not None:
                                return cod.text
                            else:
                                return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "SALES_VOUCHER":
                            return salesvouch_cod()

                for b in range(len(tag_list)):
                    if tag_list[b] == "CODE":
                        list.insert(b, salesvouch_cods())


##                def salesvouch_descs():
##                    def salesvouch_desc():
##                        for sale in root.findall(".//SALES_VOUCHER"):
##                            desc = sale.find('DESC')
##                            if desc is not None:
##                                return desc.text
##                            else:
##                                return "0"
##                    for a in root.find("./SHOPITEM"):
##                        if a.tag == "SALES_VOUCHER":
##                            return salesvouch_desc()
##
                for b in range(len(tag_list)):
                    if tag_list[b] == "DESC":
                        list.insert(b, salesvouch_descs())


                def internal_codes():
                    def internal_code():
                        internal_code = x.find("INTERNALCODEMO")
                        if internal_code is not None:
                            return internal_code.text
                        else:
                            return "0"
                    for a in root.find("./SHOPITEM"):
                        if a.tag == "INTERNALCODEMO":
                            return internal_code()

                for b in range(len(tag_list)):
                    if tag_list[b] == "INTERNALCODEMO":
                        list.insert(b, internal_codes())
                return list

            items_list.append(tuple(shopitems()))    
##                data = """
##                insert into %s(item_ids, product_nums, product_names, products, descriptions, urls, images, video_urls, prices, price_vats, vats, manufacturers, categories, other_categories, category_levels, eans, heureka_cpcs, delivery_dates, delivery_ids, delivery_prices, delivery_price_codes, param_names, vals, item_groups, availability, sales, accessory, extended_warranty_vals, extended_warranty_descriptions, special_services, sales_voucher_codes, sales_voucher_descriptions, internal_code)
##                values(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)
##                """%(table)
        tag_list = ", ".join(tag_list)
        conn.ping()
        with conn.cursor() as cursor:
            cursor.executemany("INSERT INTO {}({})VALUES({})".format(table, tag_list, values), items_list)
            conn.commit()
        print("{} Uploaded".format(table))    
        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        def tag1():
            def param1():
                item_list = []
                for x in root.findall('./SHOPITEM'):
                    def otherShop():
                        def productNums():
                            def productNum():
                                productNum = x.find('PRODUCTNO')
                                if productNum is not None:
                                    return productNum.text
                                else:
                                    return "0"
                            for a in root.find("./SHOPITEM"):
                                if a.tag == "PRODUCTNO":
                                    return productNum()
                        for param_name in x.findall(".//PARAM"):
                            def group():
                                list = []
                                param_names = param_name.find('PARAM_NAME')
                                vals = param_name.find('VAL')
                                list.append(productNums())
                                list.append(param_names.text)
                                list.append(vals.text)
                                return list
                            item_list.append(tuple(group()))
                    otherShop()
                return item_list
##                data = """
##                insert into %s(product_nums, param_names, vals)
##                values(%%s,%%s,%%s)
##                """%(other_table)
            conn.ping()
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO {}(product_nums, param_names, vals)VALUES(%s,%s,%s)".format(other_table), param1())
                conn.commit()

            def altimg1():
                item_list = []
                for x in root.findall('./SHOPITEM'):
                    def otherShop():
                        
                        def item_ids():
                            item_ids = x.find('ITEM_ID')
                            try:
                                return item_ids.text
                            except AttributeError:
                                return ""
                        
                        def images():
                            images = x.find('IMGURL')
                            try:
                                return images.text
                            except AttributeError:
                                return ""
                        for image in x.findall(".//IMGURL_ALTERNATIVE"):
                            def group():
                                list = []
                                list.append(item_ids())
                                list.append(images())
                                list.append(image.text)
                                return list
                            item_list.append(tuple(group()))
                    otherShop()                        
                return item_list
##                data = """
##                insert into %s(item_ids, images, alternative_images)
##                values(%%s,%%s,%%s)
##                """%(other_table)
            conn.ping()
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO {}(item_ids, images, alternative_images)VALUES(%s,%s,%s)".format(other_table), altimg1())
                conn.commit()

            for x in root.find("./SHOPITEM"):
                if x.tag == "PARAM":
                    return param1()
                elif x.tag == "IMGURL_ALTERNATIVE":
                    return altimg1()


        tag1()
        print("{} Uploaded".format(other_table))

        def tag2():
            def param2():
                item_list = []
                for x in root.findall('./SHOPITEM'):
                    def otherShop():                        
                        def images():
                            def image():
                                image = x.find('IMGURL')
                                if image is not None:
                                    return image.text
                                else:
                                    return "0"
                            for a in root.find("./SHOPITEM"):
                                if a.tag == "IMGURL":
                                    return image()
                        
                        for image in x.findall(".//ADDIMG/IMGURL"):
                            def group():
                                list = []
                                list.append(images())
                                list.append(image.text)
                                return list
                            item_list.append(tuple(group()))
                    otherShop()
                return item_list
                    
##                data = """
##                insert into %s(images, other_images)
##                values(%%s,%%s)
##                """%(other_table+"_1")
            conn.ping()
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO {}(images, other_images)VALUES(%s,%s)".format(other_table+ "_1"), param2())
                conn.commit()
                    
            
            for x in root.find("./SHOPITEM"):
                if x.tag == "ADDIMG":
                    return param2()
                
            
        tag2()             
        print("{} uploaded".format(other_table+"_1"))
    CreateXML()
    

    def NewXML():
        

        conn = pymysql.connect(host='' ,
                                 port=10065,
                                 user='',
                                 password='',
                                 db='')
        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        other_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag is not None:
                other_list.append(x.tag)
                for y in x:
                    if y.tag is not None:
                        other_list.append(y.tag)
        duplicate_list = list(set(duplicates(other_list)))
        tag_list = []
        for x in root.find("./SHOPITEM"):
            if x.tag not in duplicate_list:
                tag_list.append(x.tag)
            for y in x:
                if y.tag not in duplicate_list and y.tag is not None:
                    tag_list.append(y.tag)
        for x in tag_list:
            if x == "DELIVERY":
                tag_list.remove(x)
            if x == "CATEGORIES":
                tag_list.remove(x)
            if "IMGURL" not in tag_list:
                tag_list.insert(6, "IMGURL")

        def GetItemID():
            for x in range(len(tag_list)):
                if tag_list[x] == "ITEM_ID":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall()        
                        list=[]
                        for x in rows:
                            list.append("".join(str(x)))
                        return list

        def GetItem1():
            cursor=conn.cursor()
            SQL="select item_ids,images from %s"%(table)
            with conn.cursor() as cursor:
                cursor.execute(SQL)
                rows=cursor.fetchall()
                return rows

        def GetProNum():
            for x in range(len(tag_list)):
                if tag_list[x] == "PRODUCTNO":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall()
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list


        def GetProNam():
            for x in range(len(tag_list)):
                if tag_list[x] == "PRODUCTNAME":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)

                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetProduct():
            for x in range(len(tag_list)):
                if tag_list[x] == "PRODUCT":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                                list.append("".join(x))
                        return list

        def GetDesc():
            for x in range(len(tag_list)):
                if tag_list[x] == "DESCRIPTION":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list
            
        def GetCat1():
            for x in range(len(tag_list)):
                if tag_list[x] == "CATEGORYTEXT":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetUrl():
            for x in range(len(tag_list)):
                if tag_list[x] == "URL":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetImage():
            for x in range(len(tag_list)):
                if tag_list[x] == "IMGURL":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetAlt():
            cursor = conn.cursor()
            SQL = "select item_ids, alternative_images from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                return rows

        def GetPrice1():
            for x in range(len(tag_list)):
                if tag_list[x] == "PRICE":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetVid():
            for x in range(len(tag_list)):
                if tag_list[x] == "VIDEO_URL":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            try:
                                return list.append("".join(x))
                            except TypeError:
                                return " "
                        return list


        def GetPrice():
            for x in range(len(tag_list)):
                if tag_list[x] == "PRICE_VAT":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(x[0])
                        return list

        def GetVat():
            for x in range(len(tag_list)):
                if tag_list[x] == "VAT":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetMan():
            for x in range(len(tag_list)):
                if tag_list[x] == "MANUFACTURER":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetCat2():
            for x in range(len(tag_list)):
                if tag_list[x] == "CATEGORY_NAME":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            try:
                                list.append("".join(x))
                            except TypeError:
                                return ""
                        return list

        def GetCatlevel():
            for x in range(len(tag_list)):
                if tag_list[x] == "CATEGORY_LEVEL":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(str(x[0])))
                        return list


        def GetEAN():
            for x in range(len(tag_list)):
                if tag_list[x] == "EAN":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetCPC():
            for x in range(len(tag_list)):
                if tag_list[x] == "HEUREKA_CPC":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(x[0])
                        return list

        def GetDelDat():
            for x in range(len(tag_list)):
                if tag_list[x] == "DELIVERY_DATE":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(x[0])
                        return list
            
        def GetDelID():
            for x in range(len(tag_list)):
                if tag_list[x] == "DELIVERY_ID":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list

        def GetDelP():
            for x in range(len(tag_list)):
                if tag_list[x] == "DELIVERY_PRICE":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(x[0])
                        return list


        def GetDelC():
            for x in range(len(tag_list)):
                if tag_list[x] == "DELIVERY_PRICE_COD":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(str(x[0]))
                        return list

        def GetParamNam():
            for x in range(len(tag_list)):
                if tag_list[x] == "PARAM_NAME":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            try:
                                return list.append("".join(str(x)))
                            except TypeError:
                                return " "
                        return list

        def GetVals1():
            for x in range(len(tag_list)):
                if tag_list[x] == "VAL":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            try:
                                return list.append("".join(str(x)))
                            except TypeError:
                                return " "
                        return list

        def GetVals2():
            cursor = conn.cursor()
            SQL = "select vals from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetItemGroups():
            for x in range(len(tag_list)):
                if tag_list[x] == "ITEM_GROUP":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(str(x)))
                        return list

        def GetAvails():
            for x in range(len(tag_list)):
                if tag_list[x] == "AVAILABILITY":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(str(x[0])))
                        return list

        def GetSales():
            for x in range(len(tag_list)):
                if tag_list[x] == "SALE":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(str(x[0])))
                        return list

        def GetAccess():
            for x in range(len(tag_list)):
                if tag_list[x] == "ACCESS":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append(x[0])
                        return list

##        def GetExtVals():
##            cursor = conn.cursor()
##            SQL = "select extended_warranty_vals from %s"%(table)
##            
##            with conn.cursor() as cursor:         
##                cursor.execute(SQL)
##                rows = cursor.fetchall();
##                list=[]
##                for x in rows:
##                    list.append("".join(str(x)))
##                return list
##
##        def GetExtDesc():
##            cursor = conn.cursor()
##            SQL = "select extended_warranty_descriptions from %s"%(table)
##            
##            with conn.cursor() as cursor:         
##                cursor.execute(SQL)
##                rows = cursor.fetchall();
##                list=[]
##                for x in rows:
##                    list.append("".join(str(x)))
##                return list

        def GetSpecialServs():
            for x in range(len(tag_list)):
                if tag_list[x] == "SPECIAL_SERVICE":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(str(x)))
                        return list

##        def GetSalesVouchCod():
##            for x in range(len(tag_list)):
##                if tag_list[x] == "SALES_VOUCHER":
##                    name = tag_list[x]
##            cursor = conn.cursor()
##            SQL = "select {} from {}".format(name,table)
##            with conn.cursor() as cursor:         
##                cursor.execute(SQL)
##                rows = cursor.fetchall();
##                list=[]
##                for x in rows:
##                    list.append("".join(str(x)))
##                return list
##
##        def GetSalesVouchDescs():
##            cursor = conn.cursor()
##            SQL = "select sales_voucher_descriptions from %s"%(table)
##            
##            with conn.cursor() as cursor:         
##                cursor.execute(SQL)
##                rows = cursor.fetchall();
##                list=[]
##                for x in rows:
##                    list.append("".join(str(x)))
##                return list
##
        def GetInternalCodes():
            for x in range(len(tag_list)):
                if tag_list[x] == "INTERNALCODEMO":
                    name = tag_list[x]
                    cursor = conn.cursor()
                    SQL = "select {} from {}".format(name,table)
                    with conn.cursor() as cursor:         
                        cursor.execute(SQL)
                        rows = cursor.fetchall();
                        list=[]
                        for x in rows:
                            list.append("".join(x))
                        return list


        def CreateXML(fileName):
            def items():
                root=ET.Element("Shop")   
                for item in GetItemID():
                    c0=ET.Element("ShopItem")
                    root.append(c0)
                    c1=ET.Element("ItemInfo")
                    c0.append(c1)
                    c2=ET.Element("Url")
                    c0.append(c2)
                    c3=ET.Element("Image")
                    c0.append(c3)
                    c4=ET.Element("ProductInfo")
                    c0.append(c4)
                    c5=ET.Element("Delivery")
                    c0.append(c5)

                def InputItemID():            
                    list1 = GetItemID()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ItemID").text= list1[x]
                        x+=1
                
                def InputProNum():
                    list2 = GetProNum()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ProductNum").text= list2[x]
                        x+=1
                
                def InputProNam():
                    list3 = GetProNam()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ProductName").text= list3[x]
                        x+=1
                
                def InputDesc():
                    list4 = GetDesc()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Description").text= list4[x]
                        x+=1

                def InputItemGroup():
                    list20 = GetItemGroups()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Item_Group").text= str(list20[x])
                        x+=1

                def InputProducts():
                    list30 = GetProduct()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Product").text= list30[x]
                        x+=1

                def InputInternalCodes():
                    list31 = GetInternalCodes()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Internal_Code").text= list31[x]
                        x+=1


                def ItemInfo():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "ITEM_ID":
                            InputItemID()
                        if x.tag == "PRODUCTNO":
                            InputProNum()                
                        if x.tag == "DESCRIPTION":
                            return InputDesc()
                        if x.tag == "PRODUCTNAME":
                            InputProNam()
                        if x.tag == "ITEMGROUP_ID":
                            InputItemGroup()
                        if x.tag == "PRODUCT":
                            InputProducts()
                        if x.tag == "INTERNALCODEMO":
                            InputInternalCodes()


                ItemInfo()

                def InputUrl():
                    list6 = GetUrl()
                    x=0
                    for elm in root.findall(".//Url"):
                        ET.SubElement(elm, "Url").text= list6[x]
                        x+=1
                def URL():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "URL":
                            InputUrl()
                URL()
                
                def InputImage():
                    list7 = GetImage()
                    x=0
                    for elm in root.findall(".//Image"):
                        ET.SubElement(elm, "FirstImg").text= list7[x]
                        x+=1
                def Images():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "IMGURL":
                            InputImage()
                Images()

                def InputPrice():
                    list9 = GetPrice1()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Price").text= str(list9[x])
                        x+=1
                

                def InputPriceVat():
                    list9 = GetPrice()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "PriceVat").text= str(list9[x])
                        x+=1
                
                def InputVat():
                    list10= GetVat()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Vat").text= list10[x]
                        x+=1
                
                def InputMan():
                    list11= GetMan()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Manufacturer").text= list11[x]
                        x+=1
                
                def InputCat1():
                    list12 = GetCat1()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Category").text= list12[x]
                        x+=1
                
                def InputCat2():
                    list13 = GetCat2()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Other_Category").text= list13[x]
                        x+=1

                def InputCatlevel():
                    list29 = GetCatlevel()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Category_Level").text= list29[x]
                        x+=1


                def InputEAN():
                    list14 = GetEAN()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "EAN").text= list14[x]
                        x+=1
                
                def InputCPC():
                    list15 = GetCPC()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "HeurekaCPC").text= str(list15[x])
                        x+=1
                def InputAvail():
                    list21 = GetAvails()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Availability").text= str(list21[x])
                        x+=1
                
                def InputSale():
                    list22 = GetSales()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale").text= str(list22[x])
                        x+=1
                
                def InputAccess():
                    list23 = GetAccess()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Accessory").text= str(list23[x])
                        x+=1
                
                def InputExtVal():
                    list24 = GetExtVals()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Extended_Warranty_Value").text= str(list24[x])
                        x+=1
                
                def InputExtDesc():
                    list25 = GetExtDesc()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Extended_Warranty_Description").text= str(list25[x])
                        x+=1
                
                def InputSpecialServ():
                    list26 = GetSpecialServs()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Special_Service").text= str(list26[x])
                        x+=1
                
                def InputSaleVouchCod():
                    list27 = GetSalesVouchCod()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale_Voucher_Code").text= str(list27[x])
                        x+=1
                
                def InputSaleVouchDesc():
                    list28 = GetSalesVouchDescs()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale_Voucher_Description").text= str(list28[x])
                        x+=1

                        
                def ProductInfo():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "SALES_VOUCHER":
                            Items(InputSaleVouchDesc())
                        if x.tag == "SALES_VOUCHER":
                            InputSaleVouchCod()
                        if x.tag == "SPECIAL_SERVICE":
                            InputSpecialServ()
                        if x.tag == "EXTENDED_WARRANTY":
                            InputExtDesc()
                        if x.tag == "EXTENDED_WARRANTY":
                            InputExtVal()
                        if x.tag == "ACCESSORY":
                            InputAccess()
                        if x.tag == "SALE":
                            InputSale()
                        if x.tag == "AVAILABILITY":
                            InputAvail()
                        if x.tag == "HEUREKA_CPC":
                            InputCPC()
                        if x.tag == "EAN":
                            InputEAN()
                        if x.tag == "CATEGORYTEXT":
                            InputCat1()
                        if x.tag == "CATEGORIES":
                            InputCat2()
                        if x.tag == "CATEGORIES":
                            InputCatlevel()
                        if x.tag == "MANUFACTURER":
                            InputMan()
                        if x.tag == "VAT":
                            InputVat()
                        if x.tag == "PRICE_VAT":
                            InputPriceVat()
                        if x.tag == "PRICE":
                            InputPrice()

                ProductInfo()
                
                def InputDelDat():
                    list16 = GetDelDat()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryDate").text= str(list16[x])
                        x+=1
                
                def InputDelID():
                    list17 = GetDelID()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryID").text= list17[x]
                        x+=1
                
                def InputDelP():
                    list18 = GetDelP()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryPrice").text= str(list18[x])
                        x+=1
                
                def InputDelC():
                    list19 = GetDelC()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryPriceCOD").text= str(list19[x])
                        x+=1

                def Deliveries():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "DELIVERY":
                            InputDelC()
                        if x.tag == "DELIVERY":
                            InputDelP()
                        if x.tag == "DELIVERY":
                            InputDelID()
                        if x.tag == "DELIVERY_DATE":
                            InputDelDat()
                Deliveries()

                tree=ET.ElementTree(root)
                with open(fileName, "wb") as files:
                    tree.write(files)

##        if __name__=="__main__":
##            CreateXML(fileName_new)
            

            def productNumbers():
                root=ET.Element("Shop")
                for item in GetProNum():
                    c0=ET.Element("ShopItem")
                    root.append(c0)
                    c1=ET.Element("ItemInfo")
                    c0.append(c1)
                    c2=ET.Element("Url")
                    c0.append(c2)
                    c3=ET.Element("Image")
                    c0.append(c3)
                    c4=ET.Element("ProductInfo")
                    c0.append(c4)
                    c5=ET.Element("Delivery")
                    c0.append(c5)

                def InputItemID():            
                    list1 = GetItemID()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ItemID").text= list1[x]
                        x+=1
                
                def InputProNum():
                    list2 = GetProNum()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ProductNum").text= list2[x]
                        x+=1
                
                def InputProNam():
                    list3 = GetProNam()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "ProductName").text= list3[x]
                        x+=1
                
                def InputDesc():
                    list4 = GetDesc()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Description").text= list4[x]
                        x+=1

                def InputItemGroup():
                    list20 = GetItemGroups()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Item_Group").text= str(list20[x])
                        x+=1

                def InputProducts():
                    list30 = GetProduct()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Product").text= list30[x]
                        x+=1

                def InputInternalCodes():
                    list31 = GetInternalCodes()
                    x=0
                    for elm in root.findall(".//ItemInfo"):
                        ET.SubElement(elm, "Internal_Code").text= list31[x]
                        x+=1


                def ItemInfo():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "ITEM_ID":
                            InputItemID()
                        if x.tag == "PRODUCTNO":
                            InputProNum()                
                        if x.tag == "DESCRIPTION":
                            return InputDesc()
                        if x.tag == "PRODUCTNAME":
                            InputProNam()
                        if x.tag == "ITEMGROUP_ID":
                            InputItemGroup()
                        if x.tag == "PRODUCT":
                            InputProducts()
                        if x.tag == "INTERNALCODEMO":
                            InputInternalCodes()


                ItemInfo()

                def InputUrl():
                    list6 = GetUrl()
                    x=0
                    for elm in root.findall(".//Url"):
                        ET.SubElement(elm, "Url").text= list6[x]
                        x+=1
                def URL():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "URL":
                            InputUrl()
                URL()
                
                def InputImage():
                    list7 = GetImage()
                    x=0
                    for elm in root.findall(".//Image"):
                        ET.SubElement(elm, "FirstImg").text= list7[x]
                        x+=1
                def Images():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "IMGURL":
                            InputImage()
                Images()

                def InputPrice():
                    list9 = GetPrice1()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Price").text= str(list9[x])
                        x+=1
                

                def InputPriceVat():
                    list9 = GetPrice()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "PriceVat").text= str(list9[x])
                        x+=1
                
                def InputVat():
                    list10= GetVat()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Vat").text= list10[x]
                        x+=1
                
                def InputMan():
                    list11= GetMan()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Manufacturer").text= list11[x]
                        x+=1
                
                def InputCat1():
                    list12 = GetCat1()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Category").text= list12[x]
                        x+=1
                
                def InputCat2():
                    list13 = GetCat2()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Other_Category").text= list13[x]
                        x+=1

                def InputCatlevel():
                    list29 = GetCatlevel()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Category_Level").text= list29[x]
                        x+=1


                def InputEAN():
                    list14 = GetEAN()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "EAN").text= list14[x]
                        x+=1
                
                def InputCPC():
                    list15 = GetCPC()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "HeurekaCPC").text= str(list15[x])
                        x+=1
                def InputAvail():
                    list21 = GetAvails()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Availability").text= str(list21[x])
                        x+=1
                
                def InputSale():
                    list22 = GetSales()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale").text= str(list22[x])
                        x+=1
                
                def InputAccess():
                    list23 = GetAccess()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Accessory").text= str(list23[x])
                        x+=1
                
                def InputExtVal():
                    list24 = GetExtVals()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Extended_Warranty_Value").text= str(list24[x])
                        x+=1
                
                def InputExtDesc():
                    list25 = GetExtDesc()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Extended_Warranty_Description").text= str(list25[x])
                        x+=1
                
                def InputSpecialServ():
                    list26 = GetSpecialServs()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Special_Service").text= str(list26[x])
                        x+=1
                
                def InputSaleVouchCod():
                    list27 = GetSalesVouchCod()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale_Voucher_Code").text= str(list27[x])
                        x+=1
                
                def InputSaleVouchDesc():
                    list28 = GetSalesVouchDescs()
                    x=0
                    for elm in root.findall(".//ProductInfo"):
                        ET.SubElement(elm, "Sale_Voucher_Description").text= str(list28[x])
                        x+=1

                        
                def ProductInfo():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "SALES_VOUCHER":
                            Items(InputSaleVouchDesc())
                        if x.tag == "SALES_VOUCHER":
                            InputSaleVouchCod()
                        if x.tag == "SPECIAL_SERVICE":
                            InputSpecialServ()
                        if x.tag == "EXTENDED_WARRANTY":
                            InputExtDesc()
                        if x.tag == "EXTENDED_WARRANTY":
                            InputExtVal()
                        if x.tag == "ACCESSORY":
                            InputAccess()
                        if x.tag == "SALE":
                            InputSale()
                        if x.tag == "AVAILABILITY":
                            InputAvail()
                        if x.tag == "HEUREKA_CPC":
                            InputCPC()
                        if x.tag == "EAN":
                            InputEAN()
                        if x.tag == "CATEGORYTEXT":
                            InputCat1()
                        if x.tag == "CATEGORIES":
                            InputCat2()
                        if x.tag == "CATEGORIES":
                            InputCatlevel()
                        if x.tag == "MANUFACTURER":
                            InputMan()
                        if x.tag == "VAT":
                            InputVat()
                        if x.tag == "PRICE_VAT":
                            InputPriceVat()
                        if x.tag == "PRICE":
                            InputPrice()

                ProductInfo()
                
                def InputDelDat():
                    list16 = GetDelDat()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryDate").text= str(list16[x])
                        x+=1
                
                def InputDelID():
                    list17 = GetDelID()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryID").text= list17[x]
                        x+=1
                
                def InputDelP():
                    list18 = GetDelP()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryPrice").text= str(list18[x])
                        x+=1
                
                def InputDelC():
                    list19 = GetDelC()
                    x=0
                    for elm in root.findall(".//Delivery"):
                        ET.SubElement(elm, "DeliveryPriceCOD").text= str(list19[x])
                        x+=1

                def Deliveries():
                    mytree=ET.parse(user_xml)
                    root1=mytree.getroot()            
                    for x in root1.find("./SHOPITEM"):
                        if x.tag == "DELIVERY":
                            InputDelC()
                        if x.tag == "DELIVERY":
                            InputDelP()
                        if x.tag == "DELIVERY":
                            InputDelID()
                        if x.tag == "DELIVERY_DATE":
                            InputDelDat()
                Deliveries()
                                
                tree=ET.ElementTree(root)
                with open(fileName, "wb") as files:
                    tree.write(files)

            mytree=ET.parse(user_xml)
            root1=mytree.getroot()
            for x in root1.find("./SHOPITEM"):
                if x.tag == "ITEM_ID":
                    return items()
                elif x.tag == "PARAM":
                    return productNumbers()


        if __name__=="__main__":
            CreateXML(fileName_new)

        



    NewXML()
    print("{} almost Completed".format(fileName_new))

    def tag3():
        conn = pymysql.connect(host='' ,
                                 port=10065,
                                 user='',
                                 password='',
                                 db='')


        def GetAlt():
            cursor = conn.cursor()
            SQL = "select images, alternative_images from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                return rows


        def GetParamNam():
            cursor = conn.cursor()
            SQL = "select product_nums , param_names, vals from %s"%(other_table)
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x)
                return list



        def InputParam():

            def ParamNam():
                tree = ET.parse(fileName_new)
                root = tree.getroot()
                list = GetParamNam()

                for elm in root.findall(".//ItemInfo"):
                    images = elm.find(".//ProductNum")
                    for z in list:
                        if z[0] == images.text:
                                ET.SubElement(elm, "Param_Name").text=z[1]
                                ET.SubElement(elm, "Param_Value").text=z[2]                            

                tree.write(fileName_new)
                print("Param_Name/Param_Values Completed")

            def InputAltImg():
                tree = ET.parse(fileName_new)
                root = tree.getroot()

                list = GetAlt()

                for elm in root.findall(".//Image"):
                    images = elm.find(".//FirstImg")
                    for z in list:
                        if z[0] == images.text:
                            ET.SubElement(elm, "AltImg").text=z[1]


                tree.write(fileName_new)        
                print("AltImg Completed")
                
            new_tree=ET.parse(user_xml)
            new_root=new_tree.getroot()

            for x in new_root.find("./SHOPITEM"):
                if x.tag == "PARAM":
                    return ParamNam()
                elif x.tag == "IMGURL_ALTERNATIVE":
                    return InputAltImg()
        InputParam()
        
    tag3()
    def tag4():
        conn = pymysql.connect(host='' ,
                                 port=10065,
                                 user='',
                                 password='',
                                 db='')


        def GetOtherImg():
            cursor = conn.cursor()
            SQL = "select images, other_images from %s"%(other_table+"_1")
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x)
                return list


        def InputImg():
            tree = ET.parse(fileName_new)
            root = tree.getroot()

            list = GetOtherImg()



                    
            for elm in root.findall(".//Image"):
                images = elm.find(".//FirstImg")
                for z in list:
                    if z[0] == images.text:
                            ET.SubElement(elm, "SecondImg").text=z[1]

            tree.write(fileName_new)        
            print("SecondImg Completed")
        new_tree= ET.parse(user_xml)
        new_root= new_tree.getroot()
        for x in new_root.find("./SHOPITEM"):
            if x.tag == "ADDIMG":
                return InputImg()

    tag4()
    print("{} Completed".format(fileName_new))


def userinput():
    global user_xml_input
    user_xml_input = []
    global table_input
    table_input=[]
    global other_table_input
    other_table_input=[]
    global fileName_new_input
    fileName_new_input=[]
    global list_of_inputs
    list_of_inputs = [user_xml_input, table_input, other_table_input, fileName_new_input]

    while True:
        user_question = input("Put in xml, table and new file (Y/N): ")
        user_question = user_question[0].upper()
        if user_question == "Y":
            first = input("Enter XML: ")
            user_xml_input.append(first)
            second=input("Table Name: ")
            table_input.append(second)
            third=input("Other Table Name: ")
            other_table_input.append(third)
            fourth = input("Enter New XML Name: ")
            fileName_new_input.append(fourth)
        elif user_question == "N":
            break
    return user_xml_input, table_input, other_table_input, fileName_new_input

userinput()
nested_list = [[] for _ in range(len(user_xml_input))]
for x in range(len(user_xml_input)):
    for y in list_of_inputs:
        if y[x] not in nested_list[x]:
            nested_list[x].append(y[x])

for x in range(len(user_xml_input)):
    process(nested_list[x])
