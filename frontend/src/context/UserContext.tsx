'use client';

import React, { createContext, useContext, useState, useEffect } from 'react';

interface UserContextType {
  userId: string;
  setUserId: (id: string) => void;
  isAuthenticated: boolean;
}

const UserContext = createContext<UserContextType | undefined>(undefined);

export { UserContext };

export function UserProvider({ children }: { children: React.ReactNode }) {
  const [userId, setUserIdState] = useState<string>('');
  const [isHydrated, setIsHydrated] = useState(false);

  // Initialize from localStorage on client side
  useEffect(() => {
    const storedUserId = localStorage.getItem('userId');
    if (storedUserId) {
      setUserIdState(storedUserId);
    } else {
      // Generate new user ID if not exists
      const newUserId = `user_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      localStorage.setItem('userId', newUserId);
      setUserIdState(newUserId);
    }
    setIsHydrated(true);
  }, []);

  const setUserId = (id: string) => {
    setUserIdState(id);
    localStorage.setItem('userId', id);
  };

  if (!isHydrated) {
    return null;
  }

  return (
    <UserContext.Provider value={{ userId, setUserId, isAuthenticated: !!userId }}>
      {children}
    </UserContext.Provider>
  );
}

export function useUser() {
  const context = useContext(UserContext);
  if (!context) {
    throw new Error('useUser must be used within UserProvider');
  }
  return context;
}
