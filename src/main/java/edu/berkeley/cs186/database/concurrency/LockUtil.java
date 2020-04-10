package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.Stack;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     * <p>
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     * <p>
     * lockType is guaranteed to be one of: S, X, NL.
     * <p>
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // case one/two: void transaction or lockType to be deployed
        // case three: new lockType is more permissive than existing lockType
        if (transaction == null || lockType == LockType.NL || LockType.substitutable(effectiveLockType, lockType)) {
            return;
        }
        checkAncestorLockTypes(lockContext, lockType, transaction);
        actOfAcquisition(lockContext, lockType, explicitLockType, transaction);
    }

    // TODO(proj4_part2): add helper methods as you see fit
    private static void checkAncestorLockTypes(LockContext lockContext, LockType lockType,
                                               TransactionContext transaction) {
        Stack<ResourceName> ancestorInOrder = new Stack();

        LockContext parent = lockContext.parentContext();
        while (parent != null) {
            ancestorInOrder.push(parent.getResourceName());
            parent = parent.parentContext();
        }

        LockType ancestorLockType = (lockType == LockType.S) ? LockType.IS : LockType.IX;
        while (!ancestorInOrder.isEmpty()) {
            LockContext temp = LockContext.fromResourceName(lockContext.lockman, ancestorInOrder.pop());
            if (!LockType.substitutable(temp.getExplicitLockType(transaction), ancestorLockType)) {
                try {
                    temp.promote(transaction, ancestorLockType);
                } catch (NoLockHeldException e) {
                    temp.acquire(transaction, ancestorLockType);
                }
            }
        }

    }

    private static void actOfAcquisition(LockContext lockContext, LockType lockType,
                                         LockType explicitLockType, TransactionContext transaction) {
        switch (explicitLockType) {
            case X:
                return;
            case S:
                if (lockType == LockType.X) {
                    lockContext.promote(transaction, lockType);
                }
                return;
            case IS:
                lockContext.escalate(transaction);
                if (lockType == LockType.X) {
                    lockContext.promote(transaction, lockType);
                }
                return;
            case IX:
                if (lockType == LockType.S) {
                    lockContext.promote(transaction, LockType.SIX);
                } else {
                    lockContext.escalate(transaction);
                }
                return;
            case NL:
                lockContext.acquire(transaction, lockType);
                return;
            case SIX:
                if (lockType == LockType.X) {
                    lockContext.escalate(transaction);
                }
                return;
            default:
                throw new UnsupportedOperationException("invalid lock acquisition");
        }
    }


}
