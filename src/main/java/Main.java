import library.Calendar;

public class Main
{
    public static void main(String[] args) {
        Calendar c = new Calendar();

//        User user1 = new User("Jan", "Kowalski");
//        User user2 = new User("John", "Smith");
//        Thing thing1 = new Thing("Car");
//
//        ArrayList<User> usersList = new ArrayList<>();
//        usersList.add(new User("Johan", "Schmidt"));
//        usersList.add(new User("Enrico", "Palazzo"));
//
//        c.insertUsers(usersList);
//
//        ArrayList<Thing> thingArrayList = new ArrayList<>();
//        thingArrayList.add(new Thing("Plane"));
//        c.insertThings(thingArrayList);
//
//        c.insertUser(user1);
//        c.insertUser(user2);
//        c.insertThing(thing1);

        c.deleteUser(10);
        c.deleteThing(3);
        c.getUsers();
        c.getThings();

        System.out.println(c.getThingCount());
        System.out.println(c.getUserCount());

        //c.changeThingName(3, "plaaa");
        //c.findThingByID(3);

        //c.changeUserName(12, "Enriccoa");
        //c.changeUserSurname(12, "Palllazzoa");
        c.findUserByID(18);

        c.closeConnection();


    }
}