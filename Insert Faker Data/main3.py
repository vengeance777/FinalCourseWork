import categories
import rental_items
import users

while True:
    _choice = input("Mitä haluat tehdä? (1=lisää roolit\n "
                    "2=lisää käyttäjiä\n "
                    "3=lisää kategoriat\n "
                    "4=lisää ominaisuudet\n "
                    "5=lisää tuotteita\n "
                    "6=lisää ominaisuuksia tuotteisiin\n "
                    "7=vuokraa\n"
                    "q=lopeta): ")
    if _choice == 'q':
        break

    elif _choice == '1':
        print("lisätään roolit")
        users.insert_roles()
    elif _choice == '2':
        num_of_rows = input("Kuinka monta käyttäjää? (oletuksena 10): ")
        if num_of_rows == "":
            num_of_rows = 10
        else:
            num_of_rows = int(num_of_rows)
        users.insert_users(num_of_rows)

    elif _choice == '3':
        categories.insert_categories()

    elif _choice == '4':
        rental_items.insert_features()

    elif _choice == '5':
        rental_items.insert_items()

    elif _choice == '6':
        rental_items.mix_features_and_items()

    elif _choice == '7':
        rental_items.rent_items()