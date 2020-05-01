package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // use switch method to implement the chart
        switch (a) {
            case NL:
                return true;
            case S:
                return (b == IS) || (b == S) || (b == NL);
            case X:
                return (b == NL);
            case IS:
                return !(b == X) || (b == NL);
            case IX:
                return (b == IS) || (b == IX)  || (b == NL);
            case SIX:
                return (b == IS) || (b == NL);
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        switch (childLockType) {
            case S:
            case IS:
                return (parentLockType == IS) || (parentLockType == IX);
            case X:
            case IX:
                return (parentLockType == IX) || (parentLockType == SIX);
            case SIX:
                return (parentLockType == IX);
            case NL:
                return true;
            default:
                throw new UnsupportedOperationException("bask parent-children pairing");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        /*
        NL(R) Permissions: {} (empty set)
        X(R) Permissions:  {Read, Write, Read descendants, Write descendants}
        S(R) Permissions:  {Read,        Read descendants}
        IS(R) Permissions: {                                                 Intent Read descendants}
        IX(R) Permissions: {                                                 Intent Read descendants, Intent Write descendants}
        SIX(R) Permissions:{Read,        Read descendants,        (redundant)Intent Read descendants, Intent Write descendants}
         */
        switch (required) {
            case S:
                return (substitute == required) || (substitute == X) || (substitute == SIX);
            case X:
                return (substitute == required);
            case IX:
                return (substitute == required) || (substitute == SIX);
            case IS:
                return (substitute == required) || (substitute == IX) || (substitute == SIX);
            case SIX:
                return (substitute == required);
            case NL:
                return true;
            default:
                throw new UnsupportedOperationException("bask parent-children pairing");
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

