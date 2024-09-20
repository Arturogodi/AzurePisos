# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-d845488d-8461-4ee9-8b24-9bdff213d1fb
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Collection Types and Methods
# MAGIC
# MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this lesson you:<br>
# MAGIC
# MAGIC - Introduce objects and methods
# MAGIC - Create lists
# MAGIC - Use methods on new collection data types

# COMMAND ----------

# DBTITLE 0,--i18n-2d0178c2-f2b4-4746-82eb-5b81eec6be0e
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Objects
# MAGIC
# MAGIC In this lesson we are first going to look at some new functionality provided by data types, and then see how we can use that in some new data types. But before we do that, we need to look at some terminology.
# MAGIC
# MAGIC An [**object**](https://www.w3schools.com/python/python_classes.asp) is an instance of a specific data type. 
# MAGIC
# MAGIC For example, **`1`** is an Integer, so we would call it an Integer object. **`"Hello"`** is a String, so we would call it a String object.

# COMMAND ----------

# DBTITLE 0,--i18n-753edb8e-797e-4bf2-b6f4-4d4c952512a1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Methods: More Functionality
# MAGIC
# MAGIC As a reminder, data types provide **data** of some kind and **operations** we can do on that kind of data. So far, we have actually only looked at a small fraction of the operations provided by each type. 
# MAGIC
# MAGIC Data types provide special functions called [**methods**](https://www.w3schools.com/python/gloss_python_object_methods.asp) which provide more functionality. Methods are exactly like normal functions except we call them on objects and they can edit the object they are called on. We invoke a method like this:
# MAGIC
# MAGIC **`object.method_name(arguments)`**
# MAGIC
# MAGIC This is a little tricky and we have a whole lesson on these coming up, but right now all you need to know is:
# MAGIC
# MAGIC **Methods are functions provided by a data type that we can call on objects of that type. They act on the object we call them with and allow us to use more functionality provided by that data type**

# COMMAND ----------

# DBTITLE 0,--i18n-2ca5b59c-4cb8-4e87-b648-166eee0aedad
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### String Methods
# MAGIC
# MAGIC Let's take a look at an example of a method on a type we already know well: Strings. Strings provide a method called [**upper()**](https://www.w3schools.com/python/ref_string_upper.asp) which capitalizes a String.

# COMMAND ----------

greeting = "hello"
print(greeting.upper())
print(greeting)

# COMMAND ----------

# DBTITLE 0,--i18n-b83c4e97-70e0-4104-90f1-12ede96a8515
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### In-place methods
# MAGIC
# MAGIC Methods are functions that act on objects, and can either perform operations in-place (modify the underlying object it was called upon) or return a new object.
# MAGIC
# MAGIC Notice that the method **`upper()`** was not a stateful, in-place method as it returned a new string and did not modify the **`greeting`** variable. Take a look <a href="https://www.w3schools.com/python/python_ref_string.asp" target="_blank">W3Schools</a> provides information on other string methods in Python.

# COMMAND ----------

# DBTITLE 0,--i18n-bc7aa58a-5ea7-4387-96b7-189b364d56c3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Tab Completion
# MAGIC
# MAGIC If you want to see a list of methods you can apply to an object, type **`.`** after the object, then hit tab key to see a drop down menu of available methods on that object.
# MAGIC
# MAGIC Try it below on the **`greeting`** string object. Type **`greeting.`** then hit the Tab key.

# COMMAND ----------

# Type . and hit Tab
greeting

# COMMAND ----------

# DBTITLE 0,--i18n-5b3151ca-f1d4-430f-8129-3cb28816c364
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### `help()`
# MAGIC
# MAGIC While using tab completion is extremely helpful, if we use it to look through all possible methods for a given object, we might still not be certain how those methods work.
# MAGIC
# MAGIC We can look up their documentation, or we can use the [**help()**](https://www.geeksforgeeks.org/help-function-in-python/) function we saw last lesson.
# MAGIC
# MAGIC As a reminder, the **`help()`** function displays some of the documentation for the item passed into it.
# MAGIC
# MAGIC For example, when using tab completion above, we see the [**capitalize()**](https://www.w3schools.com/python/ref_string_capitalize.asp) string method, but we are not certain how it works.

# COMMAND ----------

help(greeting.capitalize)

# COMMAND ----------

greeting.capitalize()

# COMMAND ----------

# DBTITLE 0,--i18n-238070d2-fa76-43ef-a14b-2d67ef88af08
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Methods with Collection Types
# MAGIC
# MAGIC Now that we have a brief understanding of methods, let's look at some more advanced data types and the methods they provide.
# MAGIC
# MAGIC We are going to look at **collection data types** next. Like the name suggests, the data in these data types is a collection of other data types.

# COMMAND ----------

# DBTITLE 0,--i18n-d028dd16-da40-4c7d-b62e-650e154d1710
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Collection Type 1: Lists
# MAGIC
# MAGIC A list is just an ordered sequence of items. 
# MAGIC
# MAGIC It is defined as a sequence of comma separated items inside square brackets like this: **`[item1, item2, item3...]`**
# MAGIC
# MAGIC The items may be of any type, though in practice you'll usually create lists where all of the values are of the same type.
# MAGIC
# MAGIC Let's make a <a href="https://www.w3schools.com/python/python_lists.asp" target="_blank">list</a> of what everyone ate for breakfast this morning.
# MAGIC
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/2/20/Scrambed_eggs.jpg/1280px-Scrambed_eggs.jpg" width="20%" height="10%">

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list

# COMMAND ----------

# Python can tell us breakfast_list's type
type(breakfast_list)

# COMMAND ----------

# DBTITLE 0,--i18n-5ed35859-d9a4-44e0-a152-befc8855bab0
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We'll use our **`breakfast_list`** as the running example, but note that the values in a list can be of any type, as shown below.

# COMMAND ----------

# any type works
["hello", True, 1, 1.5]

# COMMAND ----------

# DBTITLE 0,--i18n-5bd3521a-76df-46d3-b5ec-07d22a896321
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### List Methods
# MAGIC
# MAGIC Now that we understand the **data** a list data type provides, let's look at some of its **functionality**.
# MAGIC
# MAGIC Something you will frequently want to do is add a new item to an existing list. 
# MAGIC
# MAGIC Lists provide a method called [**append()**](https://www.w3schools.com/python/ref_list_append.asp) to do just that. 
# MAGIC
# MAGIC **`append()`** takes in an argument of any type and edits the list it is called on so that the argument is stuck onto the end of the list. 
# MAGIC
# MAGIC Let's say after we ate our pancakes, eggs, and waffles, we also had yogurt.
# MAGIC
# MAGIC Here, we can use **`append()`** to add yogurt to our **`breakfast_list`**.

# COMMAND ----------

breakfast_list.append("yogurt")
breakfast_list

# COMMAND ----------

# DBTITLE 0,--i18n-668b2cdd-8135-439b-bf95-2880346ce49c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC **Note:** Notice here that **`append()`** is an in-place method.
# MAGIC The method does not return a new list, but rather edits the original **`breakfast_list`** object. 
# MAGIC
# MAGIC **`+`** is also defined as concatenation for lists as shown below.

# COMMAND ----------

["pancakes", "eggs"] + ["waffles", "yogurt"]

# COMMAND ----------

# DBTITLE 0,--i18n-6237eb3c-7a8b-428b-9418-4b397c163484
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC While we typically use **`append()`**, it is possible to append elements to a list using **`+`**.

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list = breakfast_list + ["yogurt"]
breakfast_list

# COMMAND ----------

# DBTITLE 0,--i18n-a4bcc594-73a3-454d-ab4d-83db2844db10
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC A useful shortcut operation for this is **`+=`**.
# MAGIC
# MAGIC **`breakfast_list`** `+=` **`["yogurt"]`** is the same thing is **`breakfast_list`** `=` **`breakfast_list`** `+` **`["yogurt"]`**.
# MAGIC
# MAGIC The **`+=`** operator works for other types as well, using their respective **`+`** operator.

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles"]
breakfast_list += ["yogurt"]
breakfast_list

# COMMAND ----------

# DBTITLE 0,--i18n-31cf815e-ec37-4313-b36c-84023b9a6f8c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### List indexing
# MAGIC
# MAGIC Often, we want to reference a specific item or items in a list. This called [list indexing](https://www.w3schools.com/python/python_lists_access.asp).
# MAGIC
# MAGIC Lists provide an operation to get the item at a certain index in the list like this:
# MAGIC
# MAGIC       list_name[index]
# MAGIC
# MAGIC In Python indices start from 0, so the first element of the list is 0, the second is 1, etc.

# COMMAND ----------

breakfast_list[0]

# COMMAND ----------

# DBTITLE 0,--i18n-b86f1c49-9cc7-4699-a329-2e60bc68a774
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We can also use negative indexing, which starts counting from right to left, starting from -1. 
# MAGIC
# MAGIC Thus, the last element of the list is -1, the second to last is -2, etc.

# COMMAND ----------

breakfast_list[-1]

# COMMAND ----------

# DBTITLE 0,--i18n-7cefd54e-7d6b-449e-99c0-48809deafc78
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We can also provide a range of indices we want to access like this:
# MAGIC
# MAGIC **`list_name[start:stop]`**
# MAGIC
# MAGIC This returns a list of the values starting at **`start`** and up to but not including **`stop`**.

# COMMAND ----------

# Note the stop index is exclusive
breakfast_list[0:2]

# COMMAND ----------

# DBTITLE 0,--i18n-bd802cdb-aece-435f-8056-151ab60ce5a7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC If we don't provide a start index, Python assumes we start at the beginning.
# MAGIC
# MAGIC If we don't provide a stop index, Python assumes we stop at the end.

# COMMAND ----------

print(breakfast_list[:2])
print(breakfast_list[1:])

# COMMAND ----------

# DBTITLE 0,--i18n-c1232cd7-3bd1-4dad-bc96-97ed5d36c5b1
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We can also change the value of an index in a list to be something new like this:

# COMMAND ----------

print(breakfast_list)
breakfast_list[0] = "sausage"

print(breakfast_list)

# COMMAND ----------

# DBTITLE 0,--i18n-b26f49c7-544d-4cd7-811b-5439f136cdbe
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC We can also use **`in`** to check if an element is in a given list. This is a boolean operation:

# COMMAND ----------

"waffles" in breakfast_list

# COMMAND ----------

# DBTITLE 0,--i18n-77f9f35a-b33f-454b-b7bb-9cc03589eb8a
# MAGIC %md
# MAGIC
# MAGIC ####  Filtering Lists
# MAGIC
# MAGIC Sometimes we need to extract specific items from a list based on certain criteria.
# MAGIC
# MAGIC You can achieve this by filtering a list, which allows us to create a new list containing only the elements that meet our defined conditions.
# MAGIC
# MAGIC Let's start with a list of breakfast items that we had this morning.

# COMMAND ----------

breakfast_list = ["pancakes", "eggs", "waffles", "milk", "yogurt", "bacon", "fruit", "cereal"]

# COMMAND ----------

# DBTITLE 0,--i18n-8430d2a8-14c2-4c03-a3a4-5e3391039920
# MAGIC %md
# MAGIC
# MAGIC Now, suppose you want to create a new list containing breakfast items made from dairy products. We'll define a list of milk products that we want to filter for.

# COMMAND ----------

milk_products = []
milk_items = ["milk", "yogurt"]

# COMMAND ----------

# DBTITLE 0,--i18n-96c3bca9-c425-497c-8264-905083de27d5
# MAGIC %md
# MAGIC
# MAGIC Let's get the milk products from the breakfast we just ate.
# MAGIC
# MAGIC We are going to use a loop to do this. Looping is a programming construct that we will cover in more detail in a different discussion. For now, just know that it provides a way to iterate over each item in a list.

# COMMAND ----------

for item in breakfast_list:
    if item in milk_items:
        milk_products.append(item)

print(milk_products)

# COMMAND ----------

# DBTITLE 0,--i18n-215a3e73-2460-4a08-880e-34430d74ab56
# MAGIC %md
# MAGIC
# MAGIC This works, but it's not very concise. You can also achieve this using the **`filter()`** method. It is a useful technique for extracting elements from a list based on specific criteria.
# MAGIC
# MAGIC Now, let's get the milk products from the breakfast we just ate, but this time, we'll use the **`filter()`** method.

# COMMAND ----------

milk_products = list(filter(milk_items.count, breakfast_list))

print(milk_products)

# COMMAND ----------

# DBTITLE 0,--i18n-ddc4a8cc-f6ca-4fd0-918f-296ad6930068
# MAGIC %md
# MAGIC
# MAGIC In this example, the **`filter()`** method iterates over each item in the **`breakfast_list`** and includes it in a new list if it matches the specified criteria. In this case, that criteria is the **`milk_items.count()`**, which returns the number of times an element is found in the list. So in otherwise, we're filtering items that can be found in the **`milk_items`** list; otherwise, the item is excluded.
# MAGIC
# MAGIC As you can see, we achieved the same result as the loop-based approach, obtaining a list of milk products from the breakfast list.

# COMMAND ----------

# DBTITLE 0,--i18n-89eac9cc-f33e-448b-9878-818def3246f6
# MAGIC %md
# MAGIC
# MAGIC #### List Comprehensions 
# MAGIC
# MAGIC List comprehensions are a concise pattern for creating new lists by applying an inline expression to each item in an existing list. They are often preferred for transforming or filtering list elements because they offer shorter, more compact, and readable code.
# MAGIC
# MAGIC Now, we already have our breakfast list. Let's use [list comprehension](https://www.w3schools.com/python/python_lists_comprehension.asp) to filter milk products from **`breakfast_list`**.

# COMMAND ----------

milk_products = [item for item in breakfast_list if item in milk_items]
print(milk_products)

# COMMAND ----------

# MAGIC  %md --i18n-8a7f0b84-0522-4036-b975-1a1f196e870a
# MAGIC  
# MAGIC  You can see here that the list comprehension filters out items from **`breakfast_list`** that are present in the **`milk_items`** list.

# COMMAND ----------

# DBTITLE 0,--i18n-589d7a85-089c-4870-abe7-7ad2fd293698
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Collection Type 2: Dictionaries
# MAGIC
# MAGIC A [Dictionary](https://www.w3schools.com/python/python_dictionaries.asp) is a sequence of key-value pairs. We define a dictionary as follows:
# MAGIC
# MAGIC `{key_1: value_1, key_2: value_2, ...}`
# MAGIC
# MAGIC The keys and values can all be of any type. However, because each key maps to a value, it is important that *all keys are unique*.
# MAGIC
# MAGIC Let's create a breakfast dictionary, where the keys are the type of food and the values are the number of those foods we ate for breakfast.

# COMMAND ----------

breakfast_dict = {"pancakes": 1, "eggs": 2, "waffles": 3}
breakfast_dict

# COMMAND ----------

# DBTITLE 0,--i18n-14aa0189-5d2a-49f5-a497-b1a29839d03c
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Dictionary Methods
# MAGIC
# MAGIC Dictionaries provide the method [**dict_object.get()**](https://www.w3schools.com/python/ref_dictionary_get.asp) to get the value in the dictionary for the given argument. 
# MAGIC
# MAGIC Let's see how many waffles we ate.

# COMMAND ----------

breakfast_dict.get("waffles")

# COMMAND ----------

# DBTITLE 0,--i18n-c8653cbc-95a1-474e-8675-bbef76473da7
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Alternatively, you can use the syntax **`dict_object[key]`**.

# COMMAND ----------

breakfast_dict["waffles"]

# COMMAND ----------

# DBTITLE 0,--i18n-de5edaf0-5374-4f84-888d-3a2793cd30fe
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC You can update a dictionary similarly to a list by assigning **`breakfast_dict[key]`** to be something. 
# MAGIC
# MAGIC If the key is present, it overwrites the current value. If not, it creates a new key-value pair. 
# MAGIC
# MAGIC Let's say we ate another waffle, bringing our total up to 4 waffles, and then ate a yogurt.

# COMMAND ----------

print(breakfast_dict)
breakfast_dict["waffles"] += 1
breakfast_dict["yogurt"] = 1
print(breakfast_dict)

# COMMAND ----------

# DBTITLE 0,--i18n-9492938e-d135-47f8-8256-804d70cc4ff8
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Notice the use of **`+=`** to increment the count of waffles.
# MAGIC
# MAGIC **Question**: Why did we not use **`+=`** to increment the yogurt count?

# COMMAND ----------

# DBTITLE 0,--i18n-8092da28-ccad-4265-a97e-fb30c6da249d
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC In order to determine if a key is in a dictionary, we can use the method [**dict_name.keys()**](https://www.w3schools.com/python/ref_dictionary_keys.asp). This returns a list of the keys in the dictionary. 
# MAGIC
# MAGIC Similar to lists, we can use **`in`** to see if our key is in the dictionary. Let's see if we ate bacon for breakfast.

# COMMAND ----------

print(breakfast_dict.keys())
print("bacon" in breakfast_dict.keys())

# COMMAND ----------

# DBTITLE 0,--i18n-8913d38b-2ed7-4e52-b86a-12ef39580edd
# MAGIC %md
# MAGIC
# MAGIC ### Collection Type 3: Tuples
# MAGIC
# MAGIC A tuple is an ordered sequence of items, just like a list.
# MAGIC
# MAGIC [Tuples](https://www.w3schools.com/python/python_tuples.asp), unlike lists and dictionaries, are immutable, meaning they cannot be changed once they are created. We define a tuple as follows: **`(item1, item2, item3, ...)`**.
# MAGIC
# MAGIC Tuples can contain items of various types, and they maintain the order of the elements.
# MAGIC
# MAGIC Let's create a breakfast tuple to keep track of the items we had for breakfast.

# COMMAND ----------

breakfast_tuple = ("pancakes", "eggs", "waffles")
breakfast_tuple

# COMMAND ----------

# DBTITLE 0,--i18n-61565c1c-7ac4-4d61-9806-57f23895b014
# MAGIC %md
# MAGIC
# MAGIC #### Tuple Methods 
# MAGIC
# MAGIC Tuples are simple and do not have many built-in methods compared to lists and dictionaries. Tuples don't have methods like **`append()`**, for example, since they are immutable. Once a tuple is created, you cannot change, add, or remove elements from it. However, you can perform operations like indexing, slicing, and checking for the presence of an element, similar to lists. Let's take a look at what we had for breakfast first using indexing.

# COMMAND ----------

breakfast_tuple[0]  

# COMMAND ----------

# DBTITLE 0,--i18n-6e731df0-6a2f-48ac-a31b-973fd2444155
# MAGIC %md
# MAGIC
# MAGIC Slicing can also be used with tuples, which allows you to extract a range of elements.

# COMMAND ----------

breakfast_tuple[1:3]

# COMMAND ----------

# DBTITLE 0,--i18n-08c904c2-be73-4b75-9e2a-2720de3bc542
# MAGIC %md
# MAGIC
# MAGIC You can use the  **`in`** operator to check if an element exists in a tuple. let's see if we had pancakes for breakfast or not.

# COMMAND ----------

print("pancakes" in breakfast_tuple)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
